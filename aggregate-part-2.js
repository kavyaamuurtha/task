
//  $project
db.users.findOne(
    {username: 'kbanker',                                   // 1
        hashed_password: 'bd1cfa194c3a603e7186780824b04419'},
    {_id: 1}                                                // 2
)

db.users.findOne(
    {username: 'kbanker',
        hashed_password: 'bd1cfa194c3a603e7186780824b04419'},
    {first_name:1, last_name:1}                               // 1
)


db.users.aggregate([
    {$match: {username: 'kbanker'}},
    {$project: {name: {first:'$first_name',
        last:'$last_name'}}
    }
])


// $group
db.orders.aggregate([
    {$project: {user_id:1, line_items:1}},
    {$unwind: '$line_items'},
    {$group: {_id: {user_id:'$user_id'}, purchasedItems: {$push: '$line_items'}}}
]).toArray();

db.orders.aggregate([
    {"$match": {"purchase_data":
    {"$gte" : new Date(2010, 0, 1)}}},
    {"$group": {
        "_id": {"year" : {"$year" :"$purchase_data"},
            "month" : {"$month" : "$purchase_data"}},
        "count": {"$sum":1},
        "total": {"$sum":"$sub_total"}}},
    {"$sort": {"_id":-1}}
]);



// $match, $sort, $skip, $limit

// PAGINATING YOUR PRODUCT REVIEWS WITH SKIP, LIMIT AND SORT
page_number = 1
product = db.products.findOne({'slug': 'wheel-barrow-9092'})

reviews = db.reviews.find({'product_id': product['_id']}).
                     skip((page_number - 1) * 12).
                     limit(12).
                     sort({'helpful_votes': -1})

                     
// same thing in aggregation framework

reviews2 = db.reviews.aggregate([
    {$match: {'product_id': product['_id']}},
    {$skip : (page_number - 1) * 12},
    {$limit: 12},
    {$sort:  {'helpful_votes': -1}}
]).toArray();


// also need these, but not shown in text
upperManhattanOrders = {'shipping_address.zip': {$gte: 10019, $lt: 10040}};

sumByUserId = {_id: '$user_id',
    total: {$sum:'$sub_total'}};

orderTotalLarge = {total: {$gt:10000}};

// shown in text
sortTotalDesc = {total: -1};

db.orders.aggregate([
    {$match: upperManhattanOrders},
    {$group: sumByUserId},
    {$match: orderTotalLarge},
    {$sort: sortTotalDesc},
    {$out: 'targetedCustomers'}
]);

// rerun previous also shown in text
db.orders.aggregate([
    {$group: sumByUserId},
    {$match: orderTotalLarge},
    {$limit: 10}
]);



// $unwind
db.products.aggregate([
    {$project : {category_ids:1}},
    {$unwind : '$category_ids'},
    {$limit: 2}
]);



// $out

// these may be needed before running query

upperManhattanOrders = {'shipping_address.zip': {$gte: 10019, $lt: 10040}};

sumByUserId = {_id: '$user_id',
    total: {$sum:'$sub_total'}};

orderTotalLarge = {total: {$gt:10000}};

sortTotalDesc = {total: -1};


// shown in text

db.orders.aggregate([
    {$match: upperManhattanOrders},
    {$group: sumByUserId},
    {$match: orderTotalLarge},
    {$sort: sortTotalDesc},
    {$out: 'targetedCustomers'}
]);

