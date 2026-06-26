# leo-connector-datalake

RStreams connector that reads queues and writes Delta tables in Databricks Unity Catalog.

## Unit tests (offline, no credentials)

```sh
cd connectors/datalake
npm install
npm run lint
npm test
```

Exit 0 is the gate. Unit tests stub the Databricks SQL client — they're fast but they do **not** catch SDK-level bugs (parameter binding, session scoping, S3 wiring). The integration suite is the real safety net before any code-shape change to `lib/connect.js` or `lib/dwconnect.js`.

## Integration tests (against the dev workspace)

```sh
npm run test:int-local   # local developer run — targets public_stage_local schema
npm run test:int-ci      # CI run — targets public_stage schema
```

Both scripts hit the same shared dev Databricks workspace (`dbc-0b0acbc9-467a`). The difference is the target schema: `test:int-local` sets `LEO_LOCAL=true` which selects `public_stage_local`; `test:int-ci` uses `public_stage`. Use `test:int-local` for day-to-day local development.

The suite **fails (non-zero exit)** when credentials aren't configured — missing credentials are a configuration error, not a skip condition. Set up once per developer:

### 1. AWS access — already in place for Dsco developers

The connector writes pipe-delimited CSV staging files to `s3://datalake-dev-641864320185-us-east-1/stage/data/internal/rithum/public_stage_local/…` cross-account from `arn:aws:iam::220162591379:role/dsco-aws-poweruser` (your default Dsco SSO role). The bucket policy that grants write is in [`data-lake-infrastructure/src/data_lake/dsco_cross_account_stack.py`](../../data-lake-infrastructure/src/data_lake/dsco_cross_account_stack.py) (`_create_dsco_developer_resource_policy`, dev+test only) — no per-developer `sts:AssumeRole` or `AWS_PROFILE` setup.

Verify your standard chain resolves to `dsco-aws-poweruser`:

```sh
aws sts get-caller-identity
# Arn should end in :assumed-role/dsco-aws-poweruser/<you>
```

### 2. Databricks credentials in `~/.databrickscfg`

The integration helper reads a named section from `~/.databrickscfg` — you choose the section name and point to it via `DATABRICKS_CONFIG_PROFILE`. The connector uses OAuth M2M (service-principal `client_id` + `client_secret`); a PAT is also supported as a fallback.

**Recommended — fetch the CI service-principal credentials from Secrets Manager, in the Data Emporium Nonprod account (641864320185), not dsco (220162591379):**

```sh
aws --profile data-emporium-nonprod secretsmanager get-secret-value \
  --secret-id data-emporium/dev/ci/infra-iac-databricks/variables/CUP_DATABRICKS_CLIENT_ID \
  --query SecretString --output text
aws --profile data-emporium-nonprod secretsmanager get-secret-value \
  --secret-id data-emporium/dev/ci/infra-iac-databricks/variables/CUP_DATABRICKS_CLIENT_SECRET \
  --query SecretString --output text
```

Then add to `~/.databrickscfg` using whatever section name you prefer (e.g. `my-dev-cup`):

```ini
[my-dev-cup]
host          = https://dbc-0b0acbc9-467a.cloud.databricks.com
client_id     = <value from CUP_DATABRICKS_CLIENT_ID>
client_secret = <value from CUP_DATABRICKS_CLIENT_SECRET>
```

Export the matching env var before running tests:

```sh
export DATABRICKS_CONFIG_PROFILE=my-dev-cup
```

**Alternative — personal access token (PAT):**

1. In the dev workspace ([dbc-0b0acbc9-467a.cloud.databricks.com](https://dbc-0b0acbc9-467a.cloud.databricks.com)), User Settings → Developer → Access tokens → Generate.
2. Add to `~/.databrickscfg`:

```ini
[my-dev-cup]
host  = https://dbc-0b0acbc9-467a.cloud.databricks.com
token = <your PAT>
```

3. `export DATABRICKS_CONFIG_PROFILE=my-dev-cup`

The connector's auth selection in [`lib/connect.js`](lib/connect.js) prefers `client_id`/`client_secret` (OAuth M2M) when both are present; otherwise it falls back to `token` (PAT). For day-to-day local dev either works; for anything that needs the same identity as the production bot, use the service-principal credentials.

### 3. Run

```sh
npm run test:int-local
```

The fixture tables live in `de_cup_dev_us.public_stage_local` and are dropped + recreated at the start of each test file.

### Overrides

Every locked default in [`test/integration/helpers/databricks.js`](test/integration/helpers/databricks.js) accepts an env-var override — useful for CI, ad-hoc workspaces, or per-developer schemas:

| Env var | Default | Source |
|---|---|---|
| `DATABRICKS_CONFIG_PROFILE` | *(required)* | which section of `~/.databrickscfg` to read |
| `DATABRICKS_HOST` | from profile | hostname (with or without `https://`) |
| `DATABRICKS_HTTP_PATH` | `/sql/1.0/warehouses/5d84579f11466e3f` | SQL warehouse HTTP path |
| `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` | from profile | OAuth M2M |
| `DATABRICKS_TOKEN` | from profile | PAT (alternative auth) |
| `DATABRICKS_CATALOG` | `de_cup_dev_us` | target Unity Catalog |
| `DATABRICKS_SCHEMA` | `public_stage_local` | target schema |
| `DATALAKE_S3_BUCKET` | `datalake-dev-641864320185-us-east-1` | staging bucket |
| `DATALAKE_S3_PREFIX` | `stage/data/internal/rithum/public_stage_local` | staging prefix |
| `AWS_REGION` | `us-east-1` | bucket region |

The host allowlist in [`helpers/databricks.js`](test/integration/helpers/databricks.js) refuses any host outside the nonprod list — neither `test:int-local` nor `test:int-ci` can accidentally hit prod.

### Troubleshooting

- **`Integration tests require Databricks credentials…`** — `DATABRICKS_CONFIG_PROFILE` not set, or the named profile section doesn't exist or has no `client_id`/`client_secret`/`token`. Set the env var to your profile section name, or supply `DATABRICKS_HOST` + auth directly as env vars.
- **`SAFETY: DATABRICKS_HOST … not in nonprod allowlist`** — host doesn't match the allowlist; either you set `DATABRICKS_HOST` to a non-dev workspace, or your profile's `host` is wrong.
- **`AccessDenied … s3:PutObject`** — your AWS chain isn't resolving to `dsco-aws-poweruser`, or the bucket policy hasn't deployed in your env. `aws sts get-caller-identity` to confirm, then check the [`dsco_cross_account_stack.py`](../../data-lake-infrastructure/src/data_lake/dsco_cross_account_stack.py) deploy status in dev.
- **`PERMISSION_DENIED … MODIFY` on `ALTER TABLE`** — the SP behind your profile lacks `MODIFY` on `de_cup_dev_us.public_stage_local`. Check Unity Catalog grants.

## Publishing

Releases are published manually to the [`leoinsights` org on npmjs.com](https://www.npmjs.com/~leoinsights). There is no automated publish workflow — the `.github/workflows/` directory does not contain one.

Before publishing a new version:

1. Run the full integration suite (`npm run test:int-local`) and confirm it passes.
2. Bump the version in `package.json` (`npm version <newversion>`).
3. Publish from the `development` branch (or a branch that is current with it):
   ```sh
   npm publish --access public
   ```
   You need an npm token with publish rights to the `leoinsights` org. The token in a developer's personal `~/.npmrc` may not have org publish access — confirm before attempting.

## Architecture

See [`docs/build_plan.md`](docs/build_plan.md) for the 12-step build sequence and the Status section that summarizes every resolved decision.

Key files:
- [`lib/connect.js`](lib/connect.js) — Databricks session + OAuth/PAT auth selection; staging pipeline via `leo-sdk.streams.toS3`; `buildStagingSelect` for inline `read_files(...)` in MERGE.
- [`lib/dwconnect.js`](lib/dwconnect.js) — factory + `importFact` + `changeTableStructure`.
- [`lib/sql.js`](lib/sql.js) — pure DDL/DML generation (`createTable`, `mergeFact`, type mapping).
- [`lib/surrogate_key.js`](lib/surrogate_key.js) — FarmFingerprint64 with unsigned→signed conversion to match Redshift `FARMFINGERPRINT64()`.
- [`lib/dol.js`](lib/dol.js) — Domain Object Layer (consumer-side queries, Databricks dialect).

## Identifier casing

All identifiers are lowercased at SQL generation time via `client.escapeId`. See build_plan.md §"Open questions" #7 for the rationale.
