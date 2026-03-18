// Q2 (MongoDB): Personalized top products using friend behavior.
// Step 1: build TARGET_USERS from events.
const TARGET_USERS = db.events.aggregate([
  { $group: { _id: "$user_id", activity: { $sum: 1 } } },
  { $sort: { activity: -1 } },
  { $limit: 100 },
  { $project: { _id: 1 } }
]).toArray().map((x) => x._id);

// Step 2: compute top-5 recommendations per user.
db.friendships.aggregate([
  { $match: { user_id: { $in: TARGET_USERS } } },
  { $project: { _id: 0, target_user_id: "$user_id", friend_user_id: "$friend_id" } },
  {
    $unionWith: {
      coll: "friendships",
      pipeline: [
        { $match: { friend_id: { $in: TARGET_USERS } } },
        { $project: { _id: 0, target_user_id: "$friend_id", friend_user_id: "$user_id" } }
      ]
    }
  },
  {
    $lookup: {
      from: "events",
      localField: "friend_user_id",
      foreignField: "user_id",
      as: "friend_events"
    }
  },
  { $unwind: "$friend_events" },
  { $match: { "friend_events.event_type": { $in: ["view", "cart", "purchase"] } } },
  {
    $group: {
      _id: {
        target_user_id: "$target_user_id",
        product_id: "$friend_events.product_id"
      },
      weighted_score: {
        $sum: {
          $switch: {
            branches: [
              { case: { $eq: ["$friend_events.event_type", "purchase"] }, then: 3 },
              { case: { $eq: ["$friend_events.event_type", "cart"] }, then: 2 }
            ],
            default: 1
          }
        }
      },
      interactions: { $sum: 1 }
    }
  },
  {
    $lookup: {
      from: "events",
      let: { uid: "$_id.target_user_id", pid: "$_id.product_id" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$user_id", "$$uid"] },
                { $eq: ["$product_id", "$$pid"] }
              ]
            }
          }
        },
        { $limit: 1 }
      ],
      as: "own_events"
    }
  },
  { $match: { own_events: { $size: 0 } } },
  { $lookup: { from: "products", localField: "_id.product_id", foreignField: "_id", as: "product" } },
  { $unwind: { path: "$product", preserveNullAndEmptyArrays: true } },
  {
    $project: {
      _id: 0,
      target_user_id: "$_id.target_user_id",
      product_id: "$_id.product_id",
      weighted_score: 1,
      interactions: 1,
      category_code: "$product.category_code",
      brand: "$product.brand"
    }
  },
  { $sort: { target_user_id: 1, weighted_score: -1, interactions: -1, product_id: 1 } },
  {
    $group: {
      _id: "$target_user_id",
      rows: {
        $push: {
          product_id: "$product_id",
          weighted_score: "$weighted_score",
          interactions: "$interactions",
          category_code: "$category_code",
          brand: "$brand"
        }
      }
    }
  },
  { $project: { rows: { $slice: ["$rows", 5] } } },
  { $unwind: { path: "$rows", includeArrayIndex: "rank" } },
  {
    $project: {
      _id: 0,
      target_user_id: "$_id",
      product_id: "$rows.product_id",
      weighted_score: "$rows.weighted_score",
      interactions: "$rows.interactions",
      category_code: "$rows.category_code",
      brand: "$rows.brand",
      rank_in_user: { $add: ["$rank", 1] }
    }
  },
  { $sort: { target_user_id: 1, rank_in_user: 1 } }
], { allowDiskUse: true });
