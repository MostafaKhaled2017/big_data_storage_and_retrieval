// Q1 (MongoDB): Check campaign conversion and social support.
db.messages.aggregate([
  {
    $lookup: {
      from: "campaigns",
      localField: "campaign_ref.campaign_key",
      foreignField: "_id",
      as: "campaign"
    }
  },
  { $unwind: "$campaign" },
  {
    $project: {
      campaign_id: "$campaign.campaign_id",
      message_type: "$campaign.campaign_type",
      channel: "$campaign.channel",
      topic: "$campaign.topic",
      user_id: 1,
      purchased: { $cond: [{ $eq: ["$engagement.is_purchased", true] }, 1, 0] }
    }
  },
  { $match: { user_id: { $ne: null } } },
  {
    $group: {
      _id: {
        campaign_id: "$campaign_id",
        message_type: "$message_type",
        channel: "$channel",
        topic: "$topic",
        user_id: "$user_id"
      },
      did_purchase: { $max: "$purchased" }
    }
  },
  {
    $lookup: {
      from: "friendships",
      let: { uid: "$_id.user_id" },
      pipeline: [
        {
          $match: {
            $expr: {
              $or: [
                { $eq: ["$user_id", "$$uid"] },
                { $eq: ["$friend_id", "$$uid"] }
              ]
            }
          }
        },
        {
          $project: {
            _id: 0,
            friend_user_id: {
              $cond: [{ $eq: ["$user_id", "$$uid"] }, "$friend_id", "$user_id"]
            }
          }
        }
      ],
      as: "friends"
    }
  },
  {
    $lookup: {
      from: "messages",
      let: {
        campaign_id: "$_id.campaign_id",
        message_type: "$_id.message_type",
        friend_ids: "$friends.friend_user_id"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$campaign_ref.campaign_id", "$$campaign_id"] },
                { $eq: ["$campaign_ref.campaign_type", "$$message_type"] },
                { $in: ["$user_id", "$$friend_ids"] },
                { $eq: ["$engagement.is_purchased", true] }
              ]
            }
          }
        },
        { $limit: 1 }
      ],
      as: "friend_purchase"
    }
  },
  {
    $addFields: {
      has_friend_purchase: { $gt: [{ $size: "$friend_purchase" }, 0] }
    }
  },
  {
    $group: {
      _id: {
        campaign_id: "$_id.campaign_id",
        message_type: "$_id.message_type",
        channel: "$_id.channel",
        topic: "$_id.topic"
      },
      recipients: { $sum: 1 },
      purchasers: { $sum: "$did_purchase" },
      social_purchasers: {
        $sum: {
          $cond: [
            {
              $and: [
                { $eq: ["$did_purchase", 1] },
                "$has_friend_purchase"
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $addFields: {
      conversion_rate_pct: {
        $round: [
          { $multiply: [{ $divide: ["$purchasers", "$recipients"] }, 100] },
          2
        ]
      },
      social_support_share_pct: {
        $round: [
          {
            $multiply: [
              {
                $divide: [
                  "$social_purchasers",
                  { $cond: [{ $eq: ["$purchasers", 0] }, 1, "$purchasers"] }
                ]
              },
              100
            ]
          },
          2
        ]
      }
    }
  },
  { $sort: { conversion_rate_pct: -1, purchasers: -1 } },
  { $limit: 20 }
], { allowDiskUse: true });
