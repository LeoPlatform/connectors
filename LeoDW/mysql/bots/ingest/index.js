"use strict";

const leo = require("leo-sdk");
const ls = leo.streams;
const moment = require("moment");
const load = require("../../../lib/load.js");

exports.handler = function(event, context, callback) {
	const ID = event.botId;
	let stats = ls.stats(event.botId, "Lead");
	let client = require("../../lib/connect.js")({
		host: "localhost",
		user: "root",
		port: 3306,
		database: "datawarehouse",
		password: "a",
		connectionLimit: 10
	});

	let tableConfig = {
		f_lead: {
			structure: {
				id: {
					nk: true,
					type: 'int'
				},
				client_id: {
					type: 'int',
					dimension: 'd_client'
				},
				community_id: {
					type: 'int',
					dimension: 'd_community'
				},
				min_bedrooms: 'int',
				max_bedrooms: 'int',
				max_price: 'int',
				min_rent: 'int',
				max_rent: 'int',
				occupants: 'int',
				desired_start: 'datetime',
				desired_end: 'datetime'
			}
		},
		d_client: {
			isDimension: true,
			structure: {
				d_id: 'sk',
				id: {
					nk: true,
					type: 'int'
				},
				name: 'varchar(70)',
				city: 'varchar(70)',
				abbreviation: 'varchar(10)',
				state: 'varchar(70)',
				country: 'varchar(70)'
			}
		},
		d_community: {
			isDimension: true,
			structure: {
				d_id: 'sk',
				id: {
					nk: true,
					type: 'int'
				},
				name: 'varchar(70)',
				city: 'varchar(70)',
				abbreviation: 'varchar(10)',
				state: 'varchar(70)',
				country: 'varchar(70)',
				units: 'int',
				year_built: 'datetime',
				floors: 'int'
			}
		},
		d_lead: {
			isDimension: true,
			structure: {
				d_id: 'sk',
				id: {
					nk: true,
					type: 'int'
				},
				status: 'varchar(70)',
				unqualified_reason: 'varchar(70)',
				unqualified_reason_id: 'int',
				first_name: 'varchar(70)',
				middle_name: 'varchar(70)',
				last_name: 'varchar(70)',
				phone_number: 'varchar(70)',
				email_address: 'varchar(70)',
				address_1: 'varchar(70)',
				address_2: 'varchar(70)',
				city: 'varchar(70)',
				abbreviation: 'varchar(10)',
				state: 'varchar(70)',
				country: 'varchar(70)',
				zip: 'varchar(20)',
				move_reason: 'varchar(70)',
				move_reason_id: 'int',
				adsource: 'int',
				adsource_name: 'varchar(50)',
				adsource_type: 'varchar(50)',

				has_min_bedrooms: 'varchar(25)',
				has_max_bedrooms: 'varchar(25)',
				has_max_price: 'varchar(25)',
				has_min_rent: 'varchar(25)',
				has_max_rent: 'varchar(25)',
				has_occupants: 'varchar(25)'
			}
		}
	};

	let mapping = ls.through((obj, done, push) => {
		let payload = obj.payload;
		push({
			payload: {
				table: 'd_client',
				data: {
					id: payload.Client.ID,
					name: payload.Client.Name,
					city: payload.Client.City,
					abbreviation: payload.Client.Abbreviation,
					state: payload.Client.State,
					country: payload.Client.Country
				}
			}
		});
		push({
			payload: {
				table: 'd_community',
				data: {
					id: payload.Community.ID,
					name: payload.Community.City,
					abbreviation: payload.Community.Abbreviation,
					state: payload.Community.State,
					country: payload.Community.Country,
					units: payload.Community.Units,
					year_built: payload.Community.YearBuilt ? moment(payload.Community.YearBuilt).format("YYYY-MM-DD HH:mm:ss") : null,
					floors: payload.Community.floors
				}
			}
		});
		push({
			payload: {
				table: 'd_lead',
				data: {
					id: payload.ID,
					status: payload.UnqualifiedReasonID ? 'Unqualified' : 'Unknown',
					unqualified_reason: payload.UnqualifiedReason || 'No reason',
					unqualified_reason_id: payload.UnqualifiedReasonID || 1,
					first_name: payload.FirstName,
					middle_name: payload.MiddleName,
					last_name: payload.LastName,
					phone_number: payload.PhoneNumber,
					email_address: payload.EmailAddress,
					address_1: payload.Address1,
					address_2: payload.Address2,
					state: payload.State,
					zip: payload.Zip,
					move_reason: payload.MoveReasonType || 'None Given',
					move_reason_id: payload.MoveReasonTypeID || 0,
					adsource: payload.Adsource.ID,
					adsource_name: payload.Adsource.Name,
					adsource_type: payload.Adsource.Type,
					has_min_bedrooms: payload.TargetHome.MinBedrooms ? 'Has Min Bedrooms' : 'No Min Bedrooms',
					has_max_bedrooms: payload.TargetHome.MaxBedrooms ? 'Has Max Bedrooms' : 'No Max Bedrooms',
					has_max_price: payload.TargetHome.MaxPrice ? 'Has Max Price' : 'No Max Price',
					has_min_rent: payload.TargetHome.UnitMinRent ? 'Has Min Rent' : 'No Min Rent',
					has_max_rent: payload.TargetHome.UnitMaxRent ? 'Has Max Rent' : 'No Max Rent',
					has_occupants: payload.TargetHome.occupants ? 'Has Occupant Limit' : 'No Occupant Limit'
				}
			}
		});
		push({
			payload: {
				table: 'f_lead',
				data: {
					id: payload.ID,
					client_id: payload.Client.ID,
					community_id: payload.Community.ID,
					min_bedrooms: payload.TargetHome.MinBedrooms,
					max_bedrooms: payload.TargetHome.MaxBedrooms,
					max_price: payload.TargetHome.MaxPrice,
					min_rent: payload.TargetHome.UnitMinRent,
					max_rent: payload.TargetHome.UnitMaxRent,
					occupants: payload.TargetHome.occupants,
					desired_start: payload.TargetHome.StartDate ? moment(payload.TargetHome.StartDate).format("YYYY-MM-DD HH:mm:ss") : null,
					desired_end: payload.TargetHome.EndDate ? moment(payload.TargetHome.EndDate).format("YYYY-MM-DD HH:mm:ss") : null
				}
			}
		});
		done();
	});

	load(client, tableConfig, ls.pipeline(leo.read(ID, "Lead", {
		stopTime: moment().add(240, "seconds"),
		start: 'z/2018/03/01'
	}), stats, mapping), err => {
		console.log(err);
		client.disconnect();
		if (!err) {
			stats.checkpoint(callback);
		} else {
			callback(err);
		}
	});
};
