// Q1

db.owid_energy_data.distinct("country",
    {$and: [
        {iso_code: {$ne: ""}},
        {iso_code: {$not: /^OWID_*/}}
        ]}).length

Q2
// maximum: 
db.owid_energy_data.find().sort({year: -1}).limit(1)

//minimum: 
db.owid_energy_data.find().sort({year: 1}).limit(1)


db.owid_energy_data.aggregate([
    {$group: {_id: {groubyCountry: "$country"}, 
        count: {$sum: 1}}},
    {$match: {count: 122}}
    ]
)

Q3

db.owid_energy_data.aggregate([{
    	$match: { 
        		"country":"Singapore",
        		"fossil_share_energy": {$lt: 100}}
},{$project:{
                   "country": 1,
                   "year": 1,
                   "fossil_share_energy": 1,
                   "low_carbon_share_energy": 1
        			}
                     }
       	 ]).limit(1)
       	 

Q4

db.owid_energy_data.aggregate([
        {$match: {"country":{$in: ["Singapore", "Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Thailand", "Vietnam"]}}},
        {$match:{"year": { $gte: 2000 , $lt: 2021 }}},
        {$match: {"gdp": {$ne: NaN}}},
        {$group: { _id:{country:"$country"},"averageGDP":{$avg:"$gdp"}}},
        {$project: {_id:0,"country":"$_id.country","averageGDP":"$averageGDP"}}
])


Q5
// first change the data type of oil_consumption and gdp
db.owid_energy_data.find().forEach( function (x) {
    x.oil_consumption = parseFloat(x.oil_consumption);
    db.owid_energy_data.save(x);
});

db.owid_energy_data.find().forEach( function (x) {
    x.gdp = parseFloat(x.gdp);
    db.owid_energy_data.save(x);
});

db.owid_energy_data.find().forEach( function (x) {
    x.year = parseInt(x.year);
    db.owid_energy_data.save(x);
});


// use windows function to segregate data and find moving averages for oil and gdp
db.owid_energy_data.aggregate([
    {$match: {
        country: {
            $in: ["Singapore", "Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Thailand", "Vietnam"]
        },
        year: {$gte: 2000, $lte: 2021}
    }},
    {$setWindowFields: {
         partitionBy: "$country",
         sortBy: { year: 1 },
         output: {
            MovingAvgforOil: {
               $avg: "$oil_consumption",
               window: {documents: [0,2]}
            },
            MovingAvgforGDP: {
                $avg: "$gdp", 
                window: {documents: [0,2]}
            }
            }
        }
    },
    {$setWindowFields: {
        partitionBy: "$country", 
        sortBy: {year:1},
        output: 
            {PrevMovingAvgforOil: 
                {$sum: "$MovingAvgforOil",
                window: {documents: [-1, -1]}
                }
            }
        }
    },
    {$match: {
        $and: [
            {MovingAvgforOil: {$ne: NaN}},
            {MovingAvgforGDP: {$ne: NaN}}]
    }
    },
    {$project: {year: 1, country: 1, MovingAvgforOil: 1, MovingAvgforGDP: 1, 
                negative_diff: {
                    $and: [{$lt: ["$MovingAvgforOil", "$PrevMovingAvgforOil"]},
                    {$ne: ["$MovingAvgforOil", null]}]
                }}
    }
])


Q6
db.energyproducts.aggregate([
    {$unwind:{ path: "$export_value", preserveNullAndEmptyArrays: true }},  
      {
        $group:{
            _id:{"energy_products":"$energy_products","sub_products":"$sub_products"}, 
            importvaluektoe:{$avg:"$value_ktoe"},
            exportvaluektoe:{$avg:"$export_value.value_ktoe"}}},
        {$sort:{"energy_products":1}}
        ])

Q7
//finding yearly difference 
db.energyproducts.aggregate([
    {$unwind:{path: "$export_value", preserveNullAndEmptyArrays: true}},
    {$project:
        {"difference":{$subtract:["$export_value.value_ktoe","$value_ktoe" ]},
        "energy_products":1, 
        "sub_products":1, 
        "year":1, 
        "value_ktoe":1, 
        "export_value.value_ktoe":1}}
])

//finding instances of more than 4 times 
db.energyproducts.aggregate([
    {$unwind:{path: "$export_value", preserveNullAndEmptyArrays: true}},
    {$match:{$expr:{$gt:["$export_value.value_ktoe","$value_ktoe"]}}},
    {$group:{_id:"$year","count":{$sum:1}}},
    {$match:{count:{$gt:4}}},
    {$project:{"year":1}}
])
    



Q8
db.householdenergy.aggregate([
    { $unwind: "$electricity_consumption" },
    {
        $project: {
            _id: 0, "electricity_consumption.Region": 1, year: {
                $convert: {
                    input: "$electricity_consumption.year",
                    to: "int",
                    onError: "Error",
                    onNull: Int32("0")
                }
            },
            kwh_per_acc: {
                $convert: {
                    input: "$electricity_consumption.kwh_per_acc",
                    to: "decimal",
                    onError: "Error",
                    onNull: Decimal128("0")
                }
            }
        }
    },
    { $match: { "electricity_consumption.Region": { $ne: "Overall" } } },
    {
        $group: {
            _id: {
                region: "$electricity_consumption.Region",
                year: "$year"
            },
            yearlyAverage: { $avg: "$kwh_per_acc" }
        }
    },
    { $sort: { "_id.region": 1, "_id.year": 1 } }])


Q9
//Generate the moving 2-year average difference & Display the top 3 regions with the most instances of negative 2-year averages
db.householdenergy.aggregate([
    { $unwind: "$electricity_consumption" },
    {
        $project: {
            _id: 0, "electricity_consumption.Region": 1, year: {
                $convert: {
                    input: "$electricity_consumption.year",
                    to: "int",
                    onError: "Error",
                    onNull: Int32("0")
                }
            },
            kwh_per_acc: {
                $convert: {
                    input: "$electricity_consumption.kwh_per_acc",
                    to: "decimal",
                    onError: "Error",
                    onNull: Decimal128("0")
                }
            }
        }
    },
    { $match: { "electricity_consumption.Region": { $ne: "Overall" } } },
    {
        $group: {
            _id: {
                region: "$electricity_consumption.Region",
                year: "$year"
            },
            yearlyAverage: { $avg: "$kwh_per_acc" }
        }
    },
    {
        $project: {
            _id: 0, Region: "$_id.region", Year: "$_id.year",
            yearlyAverage: 1
        },
    },
    {
        $setWindowFields: {
            partitionBy: "$Region",
            sortBy: { "Region": 1, "Year": 1 },
            output: {
                PreviousAndCurrentYearAvgEnergyConsumption: {
                    $push: "$yearlyAverage",
                    window: { documents: [-1, 0] }
                }
            }
        }
    },
    { $match: { $expr: { $eq: [{ $size: "$PreviousAndCurrentYearAvgEnergyConsumption" }, 2] } } },
    {
        $set: {
            MovingTwoYearAvgDiff: { $subtract: [{ $last: "$PreviousAndCurrentYearAvgEnergyConsumption" }, { $first: "$PreviousAndCurrentYearAvgEnergyConsumption" }] }
        }
    },
    {
        $group: {
            _id: {
                region: "$Region",
            },
        TotalNumberOfNegativeMovingTwoYearAvgDiff: { $sum: { $cond: [{ '$lt': ['$MovingTwoYearAvgDiff', 0] }, 1, 0] } }
        }
    },
    { $sort: { TotalNumberOfNegativeMovingTwoYearAvgDiff: -1} },
    { $limit: 3 }
])


Q10
db.householdenergy.aggregate([
    { $unwind: "$electricity_consumption" },
    {
        $project: {
            _id: 0, "electricity_consumption.Region": 1,
            year: {
                $convert: {
                    input: "$electricity_consumption.year",
                    to: "int",
                    onError: "Error",
                    onNull: Int32("0")
                }
            },
            month: {
                $convert: {
                    input: "$electricity_consumption.month",
                    to: "int",
                    onError: "Error",
                    onNull: Int32("0")
                }
            },
            kwh_per_acc: {
                $convert: {
                    input: "$electricity_consumption.kwh_per_acc",
                    to: "decimal",
                    onError: "Error",
                    onNull: Decimal128("0")
                }
            }
        }
    },
    { $match: { "electricity_consumption.Region": { $ne: "Overall" } } },
{ $match: { month: { $in: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] } } },
{
    $project: {
        _id: 0, "electricity_consumption.Region": 1, year: 1, month: 1, kwh_per_acc: 1,
        quarter: {
            $switch:
            {
                branches: [
                    {
                        case: {
                            $and: [{ $gte: ["$month", 1] },
                            { $lte: ["$month", 3] }]
                        },
                        then: 1
                    },
                    {
                        case: {
                            $and: [{ $gte: ["$month", 4] },
                            { $lte: ["$month", 6] }]
                        },
                        then: 2
                    },
                    {
                        case: {
                            $and: [{ $gte: ["$month", 7] },
                            { $lte: ["$month", 9] }]
                        },
                        then: 3
                    }
                ],
                default: 4
            }
        }
    }
},
{
    $group: {
        _id: {
            Region: "$electricity_consumption.Region",
            Year: "$year",
            Quarter: "$quarter"
        },
        QuarterlyAverage: { $avg: "$kwh_per_acc" }
    }
},
{ $sort: { "_id.Region": 1, "_id.Year": 1, "_id.Quarter": 1 } }
])


Q11
db.householdenergy.aggregate([
    {
        $project: {
            _id: 0,
            sub_housing_type:
                { $toLower: { $trim: { input: "$sub_housing_type" } } },
            year: {
                $convert: {
                    input: "$year",
                    to: "int",
                    onError: "Error",
                    onNull: Int32("0")
                }
            },
            month: {
                $convert: {
                    input: "$month",
                    to: "int",
                    onError: "Error",
                    onNull: Int32("0")
                }
            },
            avg_mthly_hh_tg_consp_kwh: {
                $convert: {
                    input: "$avg_mthly_hh_tg_consp_kwh",
                    to: "decimal",
                    onError: "Error",
                    onNull: Decimal128("0")
                }
            }
        }
    },
    { $match: { Region: { $ne: "Overall" } } },
    { $match: { month: { $in: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] } } },
    {
        $project: {
            _id: 0, sub_housing_type: 1, year: 1, month: 1, avg_mthly_hh_tg_consp_kwh: 1,
            quarter: {
                $switch:
                {
                    branches: [
                        {
                            case: {
                                $and: [{ $gte: ["$month", 1] },
                                { $lte: ["$month", 3] }]
                            },
                            then: 1
                        },
                        {
                            case: {
                                $and: [{ $gte: ["$month", 4] },
                                { $lte: ["$month", 6] }]
                            },
                            then: 2
                        },
                        {
                            case: {
                                $and: [{ $gte: ["$month", 7] },
                                { $lte: ["$month", 9] }]
                            },
                            then: 3
                        }
                    ],
                    default: 4
                }
            }
        }
    },
    {
        $group: {
            "_id": {
                Sub_housing_type: "$sub_housing_type",
                Year: "$year",
                Quarter: "$quarter"
            },
            QuarterlyAverage: { $avg: "$avg_mthly_hh_tg_consp_kwh" }
        }
    },
    { $sort: { "_id.Sub_housing_type": 1, "_id.Year": 1, "_id.Quarter": 1 } }
])

Q13: 

//* We will we will examine countries in different stages of development, namely a less-developed nation, 
//a developing nation as well as a developed country, Afghanistan, China as well as the United States. 
//Through data analysis between year 2000 to 2018. *?
/* We will be looking at 2 key variables, renewable_share_elec, which represent the share of electricity 
generation that comes from renewables. 
AND GDP which is a sign of economic growth. */

//data used for Afghanistan
db.owid_energy_data.find(
    {$and: [
      {"iso_code": "AFG"},
      {"year": {$lt: 2019}},
      {"year": {$gt: 1999}}
      ]} , 
    {"country": 1,
    "year": 1,
    "gdp":1,
    "renewables_share_energy":1,
     "_id":0}
)



//data used for China
db.owid_energy_data.find({
  "$and": [
    {
      "iso_code": "CHN"
    },
    {
      "year": {
        "$gte": 2000
      }
    },
    {
      "year": {
        "$lte": 2018
      }
    }
  ]
} , {
  "_id": 0,
  "year": 1,
  "country": 1,
  "gdp": 1,
  "renewables_share_elec": 1
})

//data used for United States of America
db.owid_energy_data.find({
  "$and": [
    {
      "iso_code": "USA"
    },
    {
      "year": {
        "$gte": 2000
      }
    },
    {
      "year": {
        "$lte": 2018
      }
    }
  ]
} , {
  "_id": 0,
  "year": 1,
  "country": 1,
  "gdp": 1,
  "renewables_share_elec": 1
})

Q14
//Convert datatype for year
db.owid_energy_data.find().forEach(function(x) {
    x.year = parseInt(x.year);
    db.owid_energy_data.save(x);
});

//Data for Figure 1: Change in Share of Fossil Fuels in Singapore’s Energy Mix from 2010 to 2021
db.owid_energy_data.find({
    "$and": [{
        "country": "Singapore"
    }, {
        "$and": [{
            "year": { "$gte": 2010 }
        }, {
            "year": { "$lte": 2021 }
        }]
    }]
}, {
    "year": 1,
    "fossil_share_elec": 1,
    "_id": 0
}
);

//Data for Figure 2: Singapore’s Electricity Demand from 2010 to 2021
db.owid_energy_data.find({
    "$and": [{
        "country ": "Singapore"
    }, {
        "$and": [{
            "year": { "$gte": 2010 }
        }, {
            "year": { "$lte": 2021 }
        }]
    }]
}, {
    "year": 1,
    "electricity_demand": 1,
    "_id": 0
}
);

//Data for Figure 3: Singapore’s GDP from 2010 to 2018
db.owid_energy_data.find({

    "$and": [{
        "country": "Singapore"
    }, {
        "$and": [{
            "year": { "$gte": 2010 }
        }, {
            "year": { "$lte": 2021 }
        }]
    }]
}, {
    "year": 1,
    "gdp": 1,
    "_id": 0
}
);

//Data for Figure 4: Singapore’s Fossil Fuel Consumption from 2010 to 2021
db.owid_energy_data.find({

    "$and": [{
        "country ": "Singapore"
    }, {
        "$and": [{
            "year": { "$gte": 2010 }
        }, {
            "year": { "$lte": 2021 }
        }]
    }]
}, {
    "year": 1,
    "fossil_cons_per_capita": 1,
    "_id": 0
}
);


//Data for Figure 5: Singapore’s Greenhouse Gas Emissions from 2010 to 2021
db.owid_energy_data.find({

    "$and": [{
        "country": "Singapore"
    }, {
        "$and": [{
            "year": { "$gte": 2010 }
        }, {
            "year": { "$lte": 2021 }
        }]
    }]
}, {
    "year": 1,
    "greenhouse_gas_emissions": 1,
    "_id": 0
}
);


//Data for Figure 6: Share of Wind Energy and Solar Energy in Singapore's Energy Mix from 2010 to 2021
db.owid_energy_data.find({
    "$and": [{
        "country": "Singapore"
    }, {
        "$and": [{
            "year": { "$gte": 2010 }
        }, {
            "year": { "$lte": 2021 }
        }]
    }]
}, {
    "year": 1,
    "wind_share_elec": 1,
    "solar_share_elec": 1,
    "_id": 0
}
);

