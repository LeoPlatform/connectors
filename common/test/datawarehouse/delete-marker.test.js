const { expect } = require("chai");
const deleteMarkerData = require("../../datawarehouse/delete-marker.js");

// The value written under the table's natural-key column is the marker's combine
// group. Two markers that land in the same group are collapsed by combine, and all
// but one are lost — so "distinct per deleted id" is a data-integrity property here,
// not a cosmetic one (RPL-6780).
describe("deleteMarkerData", () => {
	describe("natural key is `id` (every table in the RPL-6780 exposure list)", () => {
		it("a delete by the natural key carries the real key value", () => {
			expect(deleteMarkerData("id", "R1", "id"))
				.to.deep.equal({ __leo_delete__: "id", __leo_delete_id__: "R1", id: "R1" });
		});

		it("a delete by a parent FK carries a `_del_`-prefixed value, isolating it", () => {
			expect(deleteMarkerData("order_id", 555, "id"))
				.to.deep.equal({ __leo_delete__: "order_id", __leo_delete_id__: 555, id: "_del_555" });
		});
	});

	describe("natural key is NOT `id` (f_shipping_label_package, nk `package_id`)", () => {
		it("a delete by the natural key populates that column, not `id`", () => {
			// Was: { id: '_del_PKG-1', ... } with package_id left undefined, so every
			// delete in the batch hashed to the same combine group and only one survived.
			expect(deleteMarkerData("package_id", "PKG-1", "package_id"))
				.to.deep.equal({ __leo_delete__: "package_id", __leo_delete_id__: "PKG-1", package_id: "PKG-1" });
		});

		it("a delete by another column still populates the natural-key column", () => {
			expect(deleteMarkerData("shipping_label_id", "L9", "package_id"))
				.to.deep.equal({ __leo_delete__: "shipping_label_id", __leo_delete_id__: "L9", package_id: "_del_L9" });
		});

		it("distinct ids produce distinct combine groups", () => {
			const a = deleteMarkerData("package_id", "PKG-1", "package_id");
			const b = deleteMarkerData("package_id", "PKG-2", "package_id");
			expect(a.package_id).to.not.equal(b.package_id);
		});
	});

	describe("natural key unknown or composite", () => {
		it("falls back to the historical `id` shape rather than guessing a column", () => {
			expect(deleteMarkerData("id", "R1", null))
				.to.deep.equal({ __leo_delete__: "id", __leo_delete_id__: "R1", id: "R1" });
			expect(deleteMarkerData("other_id", "R1", null))
				.to.deep.equal({ __leo_delete__: "other_id", __leo_delete_id__: "R1", id: "_del_R1" });
		});
	});
});
