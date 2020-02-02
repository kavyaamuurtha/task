
// Products, categories, reviews
// Find reviews of a specific product
product = db.products.findOne({ 'slug': 'wheel-barrow-9092' })
reviews_count = db.reviews.count({ 'product_id': product['_id'] })

// start with summary for all products
ratingSummary = db.reviews.aggregate([
    {
        $group: {
            _id: '$product_id',
            count: { $sum: 1 }
        }
    }
]).next();



// rating summary - for selected product
// look up product first
product = db.products.findOne({ 'slug': 'wheel-barrow-9092' })

ratingSummary = db.reviews.aggregate([
    { $match: { product_id: product['_id'] } },
    {
        $group: {
            _id: '$product_id',
            count: { $sum: 1 }
        }
    }
]).next();

// Adding average review 

ratingSummary = db.reviews.aggregate([
    { $match: { 'product_id': product['_id'] } },
    {
        $group: {
            _id: '$product_id',
            average: { $avg: '$rating' },
            count: { $sum: 1 }
        }
    }
]).next();


// With group first

// below verifies that in fact, the first command will use an index, second will not

db.reviews.ensureIndex({ product_id: 1 })

countsByRating = db.reviews.aggregate([
    { $match: { 'product_id': product['_id'] } },
    {
        $group: {
            _id: '$rating',
            count: { $sum: 1 }
        }
    }
], { explain: true })

countsByRating = db.reviews.aggregate([
    {
        $group: {
            _id: { 'product_id': '$product_id', rating: '$rating' },
            count: { $sum: 1 }
        }
    },
    { $match: { '_id.product_id': product['_id'] } }
], { explain: true })


ratingSummary = db.reviews.aggregate([
    {
        $group: {
            _id: '$product_id',
            average: { $avg: '$rating' },
            count: { $sum: 1 }
        }
    },
    { $match: { '_id': product['_id'] } }
]).next();


// Counting Reviews by Rating
countsByRating = db.reviews.aggregate([
    { $match: { 'product_id': product['_id'] } },
    {
        $group: {
            _id: '$rating',
            count: { $sum: 1 }
        }
    }
]).toArray();

// Joining collections
db.products.aggregate([
    {
        $group: {
            _id: '$main_cat_id',
            count: { $sum: 1 }
        }
    }
]);



// "join" main category summary with categories
db.mainCategorySummary.remove({});

db.products.aggregate([
    {
        $group: {
            _id: '$main_cat_id',
            count: { $sum: 1 }
        }
    }
]).forEach(function (doc) {
    var category = db.categories.findOne({ _id: doc._id });
    if (category !== null) {
        doc.category_name = category.name;
    }
    else {
        doc.category_name = 'not found';
    }
    db.mainCategorySummary.insert(doc);
})

// findOne on mainCategorySummary

db.mainCategorySummary.findOne()

// Faster Joins - $unwind

// FASTER JOIN - $UNWIND
db.products.aggregate([
    { $project: { category_ids: 1 } },
    { $unwind: '$category_ids' },
    {
        $group: {
            _id: '$category_ids',
            count: { $sum: 1 }
        }
    },
    { $out: 'countsByCategory' }
]);

//  related findOne() - Using $out to create new collections
db.countsByCategory.findOne()

// $out and $project section

db.products.aggregate([
    {
        $group: {
            _id: '$main_cat_id',
            count: { $sum: 1 }
        }
    },
    { $out: 'mainCategorySummary' }
]);


db.products.aggregate([
    { $project: { category_ids: 1 } }
]);

//  User and Order
db.reviews.aggregate([
    {
        $group:
        {
            _id: '$user_id',
            count: { $sum: 1 },
            avg_helpful: { $avg: '$helpful_votes' }
        }
    }
])


// summarizing sales by year and month
db.orders.aggregate([
    {
        "$match": {
            "purchase_data":
                { "$gte": new Date(2010, 0, 1) }
        }
    },
    {
        "$group": {
            "_id": {
                "year": { "$year": "$purchase_data" },
                "month": { "$month": "$purchase_data" }
            },
            "count": { "$sum": 1 },
            "total": { "$sum": "$sub_total" }
        }
    },
    { "$sort": { "_id": -1 } }
]);



// Finding best manhattan customers
upperManhattanOrders = { 'shipping_address.zip': { $gte: 10019, $lt: 10040 } };

sumByUserId = {
    _id: '$user_id',
    total: { $sum: '$sub_total' }
};

orderTotalLarge = { total: { $gt: 10000 } };

sortTotalDesc = { total: -1 };

db.orders.aggregate([
    { $match: upperManhattanOrders },
    { $group: sumByUserId },
    { $match: orderTotalLarge },
    { $sort: sortTotalDesc }
]);

db.orders.aggregate([
    { $group: sumByUserId },
    { $match: orderTotalLarge },
    { $limit: 10 }
]);



// easier to modify - example - add count
sumByUserId = {
    _id: '$user_id',
    total: { $sum: '$sub_total' },
    count: { $sum: 1 }
};

// rerun previous
db.orders.aggregate([
    { $group: sumByUserId },
    { $match: orderTotalLarge },
    { $limit: 10 }
]);


db.orders.aggregate([
    { $match: upperManhattanOrders },
    { $group: sumByUserId },
    { $match: orderTotalLarge },
    { $sort: sortTotalDesc },
    { $out: 'targetedCustomers' }
]);

// fixed: added order with upper manhattan shipping address
upperManhattanOrders = { 'shipping_address.zip': { $gte: 10019, $lt: 11216 } };

db.orders.aggregate([
    { $match: upperManhattanOrders },
    { $group: sumByUserId },
    { $match: orderTotalLarge },
    { $sort: sortTotalDesc },
    { $out: 'targetedCustomers' }
]);

db.targetedCustomers.findOne();

