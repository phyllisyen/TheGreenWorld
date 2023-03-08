// nosql implementation 

// import and export of energy product to be joined into one collection with exports being a nested document in imports

// convert data type 
db.energyproducts.update(
    {"export_value.value_ktoe": {$type: "string"}},
    {$set: {"export_value.value_ktoe": parseFloat("export_value.value_ktoe")}})
    
db.energyproducts.find().forEach(function (x) {   
  x.value_ktoe = parseFloat(x.value_ktoe); // convert field to double
  db.energyproducts.save(x);
});

db.importsofenergyproducts.aggregate([
    {$lookup: {
        from: "exportsofenergyproducts",
        let: {year: "$year", energy_products: "$energy_products", sub_products: "$sub_products"},
        pipeline: [{
            $match: {
                $expr: {$and:
                    [
                        {$eq: ["$year", "$$year"]},
                        {$eq: ["$sub_products", "$$sub_products"]},
                        {$eq: ["$energy_products", "$$energy_products"]}
                    ]
            }
        }}],
        as: "export_value"}},
    {$unwind: {path: "$export_value", preserveNullAndEmptyArrays: true}},
    {$out: {db: "greenworld2022", coll: "energyproducts"}}
]);

db.importsofenergyproducts.deleteMany({})
db.exportsofenergyproducts.deleteMany({})

// household energy consumption. gas and electricity usage values are to be nested together in one collection

// for householdtowngasconsumption we find that kwh_per_acc has an irrelevant data "\r", need to remove it
// also delete records where region equal to description and month equal to "Annual"

// remove contaminated data
db.householdtowngasconsumption.deleteMany({"month": {$nin: ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "na"]}})

// remove "\r"
db.householdtowngasconsumption.aggregate([
    {$project: {
        avg_mthly_hh_tg_consp_kwh: { 
            $substr: [ 
                "$avg_mthly_hh_tg_consp_kwh", 0, { 
                    $indexOfBytes: ["$avg_mthly_hh_tg_consp_kwh", "\r"]} 
                    ]},
    "_id": 1, "year": 1, "month": 1, "sub_housing_type": {$toLower: "$sub_housing_type"}, "housing_type": 1}},
    {$out: {db: "greenworld2022", coll: "temp_coll_gas1"}}
])

// convert avg_mthly_hh_tg_consp_kwh to float
db.temp_coll_gas1.find().forEach( function (x) {
    x.avg_mthly_hh_tg_consp_kwh = parseFloat(x.avg_mthly_hh_tg_consp_kwh);
    db.temp_coll_gas1.save(x);
});

// for householdelectricityconsumption we find that kwh_per_acc has an irrelevant data "\r", need to remove it

// only take regional data 
db.householdelectricityconsumption.deleteMany({"Description": {$nin: ["North Region", "North East Region", "West Region", "East Region", "Central Region"]}})


// remove "\r"
db.householdelectricityconsumption.aggregate([
    {$project: {
        kwh_per_acc: { 
            $substr: [ 
                "$kwh_per_acc", 0, { 
                    $indexOfBytes: ["$kwh_per_acc", "\r"]} 
                    ]},
    "_id": 1, "month": 1, "dwelling_type": {$toLower: "$dwelling_type"}, "Region": 1, "year": 1}},
    {$out: {db: "greenworld2022", coll: "temp_coll_elec1"}}
])

// convert kwh_per_acc to float
db.temp_coll_elec1.find().forEach( function (x) {
    x.kwh_per_acc = parseFloat(x.kwh_per_acc);
    db.temp_coll_elec1.save(x);
    
    
// join collections
db.temp_coll_gas1.aggregate([
    {$lookup: {
        from: "temp_coll_elec1",
        let: {year: "$year", month: "$month", type: "$sub_housing_type"},
        pipeline: [{
            $match: {
                $expr: {
                    $and:
                        [
                            {$eq: ["$$year", "$year"]},
                            {$eq: ["$$month", "$month"]},
                            {$eq: ["$$type", "$dwelling_type"]}
                        ]
            }
        }}],
        as: "electricity_consumption"}
    },
     {$out: {db: "greenworld2022", coll: "householdenergy"}}
])


db.householdenergy.find()

db.householdtowngasconsumption.deleteMany({})
db.householdelectricityconsumption.deleteMany({})
db.temp_coll_gas1.deleteMany({})
db.temp_coll_elec1.deleteMany({})


// conversion of data type for owid_energy_product
db.owid_energy_data.find().forEach( function (x) {
    x.gdp = parseFloat(x.gdp);
    db.owid_energy_data.save(x);
})

db.owid_energy_data.find().forEach( function (x) {
    x.fossil_share_energy = parseFloat(x.fossil_share_energy);
    db.owid_energy_data.save(x);
})


db.owid_energy_data.find().forEach( function (x) {
    x.year = parseInt(x.year);
    db.owid_energy_data.save(x);
})


db.owid_energy_data.find().forEach( function (x) {
    x.population = parseInt(x.population);
    db.owid_energy_data.save(x);
})


db.owid_energy_data.find().forEach( function (x) {
    x.oil_consumption = parseFloat(x.oil_consumption);
    db.owid_energy_data.save(x);
});



//Q6
   
db.energyproducts.aggregate([
    {$group:{
        _id:{"energy_products":"$energy_products","sub_products":"$sub_products"}, 
        importvaluektoe:{$avg:"$value_ktoe"},
        exportvaluektoe:{$avg:"$export_value.value_ktoe"}}},
    {$sort:{"energy_products":1}}
])


//Q7

//finding yearly difference 
db.energyproducts.aggregate([
    {$project:
        {"difference":{$subtract:["$export_value.value_ktoe","$value_ktoe" ]},
        "energy_products":1, 
        "sub_products":1, 
        "year":1, 
        "value_ktoe":1, 
        "export_value.value_ktoe":1}}
])
