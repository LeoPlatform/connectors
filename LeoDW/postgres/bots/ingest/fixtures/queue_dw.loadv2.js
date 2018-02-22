let data = [];

const MAX = 100000;
for (i = 0; i < MAX; i++) {

	data.push({
		table: 'orders',
		data: {
			id: i,
			Order: i,
			Presenter: 6706991,
			'User:Purchaser': 6706991,
			'Presenter Rank': '6706991-201710',
			Party: 8055890,
			Market: 6,
			'Market:Presenter Market': 6,
			'TS:Started': '2017-10-28T10:59:29+00:00',
			'TS:Completed': '2017-10-28T10:59:32+00:00',
			TS: '2017-10-28T10:59:32+00:00',
			'TS:Shipped': '2017-10-30T15:09:24+00:00',
			'TS:Delivered': null,
			'TS:Royalty Paid': '2017-10-28T10:59:32+00:00',
			type: 'Standard',
			presenter_commissionable_amount: 73707,
			market_commissionable_amount: 85500,
			commissionable_amount: 4900,
			discount_amount: 0,
			points: 49,
			ycash: 0,
			subtotal_amount: 85500,
			shipping_cost_amount: 7500,
			declared_value_amount: 0,
			vat_amount: 11793,
			taxes_amount: 0,
			total_amount: 93000,
			weight: 0,
			items: 0,
			royalty_paid_amount: 17738,
			round_up_amount: 0,
			status: 'Processing',
			secondary_status: null,
			gti_status: null
		}
	});
	data.push({
		table: 'order_items',
		data: {
			id: i + 100000000,
			Order: i + 100000000,
			Presenter: 6706991,
			'User:Purchaser': 6706991,
			'Presenter Rank': '6706991-201710',
			Party: 8055890,
			Market: 6,
			'Market:Presenter Market': 6,
			'TS:Started': '2017-10-28T10:59:29+00:00',
			'TS:Completed': '2017-10-28T10:59:32+00:00',
			TS: '2017-10-28T10:59:32+00:00',
			'TS:Shipped': '2017-10-30T15:09:24+00:00',
			'TS:Delivered': null,
			'TS:Royalty Paid': '2017-10-28T10:59:32+00:00',
			type: 'Standard',
			presenter_commissionable_amount: 73707,
			market_commissionable_amount: 85500,
			commissionable_amount: 4900,
			discount_amount: 0,
			points: 49,
			ycash: 0,
			subtotal_amount: 85500,
			shipping_cost_amount: 7500,
			declared_value_amount: 0,
			vat_amount: 11793,
			taxes_amount: 0,
			total_amount: 93000,
			weight: 0,
			items: 0,
			royalty_paid_amount: 17738,
			round_up_amount: 0,
			status: 'Processing',
			secondary_status: null,
			gti_status: null
		}
	});
}

module.exports = data;

/*

create table order_items(
	id integer primary key,
	order_id  integer,
	presenter_id integer,
	purchaser_id integer,
	presenter_rank_id varchar(20),
	party_id integer,
	market_id integer,
	presenter_market_id integer,
	started_ts timestamp,
	completed_ts timestamp,
	ts timestamp,
	shipped_ts timestamp,
	royalty_paid_ts timestamp,
	type varchar(20),
	presenter_commissionable_amount integer,
	market_commissionable_amount integer,
	commissionable_amount integer,
	discount_amount integer,
	points integer,
	ycash integer,
	subtotal_amount integer,
	shipping_cost_amount integer,
	declared_value_amount integer,
	vat_amount integer,
	taxes_amount integer,
	total_amount integer,
	weight integer,
	items integer,
	royalty_paid_amount integer,
	round_up_amount integer,
	status varchar(20),
	secondary_status varchar(20),
	gti_status varchar(20)
);




  create table orders (
  id integer  primary key,  
  order_id integer, 
  presenter_id integer, 
  purchaser_id integer, 
  presenter_rank_id varchar(20), 
  party_id integer, 
  market_id integer, 
  presenter_market_id integer, 
  started_ts timestamp, 
  completed_ts timestamp, 
  ts timestamp, 
  shipped_ts timestamp, 
  royalty_paid_ts timestamp, 
  type varchar(20), 
  presenter_commissionable_amount integer, 
  market_commissionable_amount integer, 
  commissionable_amount integer, 
  discount_amount integer, 
  points integer, 
  ycash integer, 
  subtotal_amount integer, 
  shipping_cost_amount integer, 
  declared_value_amount integer, 
  vat_amount integer, 
  taxes_amount integer, 
  total_amount integer, 
  weight integer, 
  items integer, 
  royalty_paid_amount integer, 
  round_up_amount integer, 
  status varchar(20), 
  secondary_status varchar(20), 
  gti_status varchar(20)
  );

  */