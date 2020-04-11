//1
db.Players.find(
  {
    $and: [
      { team: /.*ia.*/ },
      { minutes: { $lt: 200 } },
      { passes: { $gt: 100 } }
    ]
  },
  { surname: 1 }
);

//2
db.Players.find({ shots: { $gt: 20 } }).sort({ shots: -1 });

//3
db.Players.aggregate([
  {
    $lookup: {
      from: "Teams",
      localField: "team",
      foreignField: "team",
      as: "Teams"
    }
  },
  {
    $match: {
      $and: [{ position: "goalkeeper" }, { "Teams.games": { $gt: 4 } }]
    }
  },
  { $project: { surname: 1, team: 1, minutes: 1 } }
]);

//4
db.Players.aggregate([
  {
    $lookup: {
      from: "Teams",
      localField: "team",
      foreignField: "team",
      as: "Teams"
    }
  },
  {
    $match: {
      $and: [{ "Teams.ranking": { $lt: 10 } }, { minutes: { $gt: 350 } }]
    }
  },
  { $count: "superstar" }
]);

//5
db.Players.aggregate([
  { $group: { _id: "$position", avg: { $avg: "$passes" } } },
  { $match: { $or: [{ _id: "midfielder" }, { _id: "forward" }] } },
  { $project: { _id: 0, Position: "$_id", "Average Passes": "$avg" } }
]);

//6
db.Teams.aggregate([
  {
    $lookup: {
      from: "Teams",
      let: {
        id2: "$_id",
        team2: "$team",
        goalsFor2: "$goalsFor",
        goalsAgainst2: "$goalsAgainst"
      },
      pipeline: [
        {
          $match: {
            $and: [
              { $expr: { $lt: ["$_id", "$$id2"] } },
              { $expr: { $ne: ["$team", "$$team2"] } },
              { $expr: { $eq: ["$goalsFor", "$$goalsFor2"] } },
              { $expr: { $eq: ["$goalsAgainst", "$$goalsAgainst2"] } }
            ]
          }
        }
      ],
      as: "team2"
    }
  },
  { $unwind: "$team2" },
  { $addFields: { against_team: "$team2.team" } },
  { $project: { team: 1, goalsFor: 1, goalsAgainst: 1, against_team: 1 } }
]);

//7
db.Teams.aggregate([
  { $project: { team: 1, ratio: { $divide: ["$goalsFor", "$goalsAgainst"] } } },
  { $sort: { ratio: -1 } },
  { $limit: 1 }
]);

//8
db.Teams.aggregate([
  {
    $lookup: {
      from: "Players",
      localField: "team",
      foreignField: "team",
      as: "p"
    }
  },
  { $unwind: "$p" },
  { $match: { "p.position": "defender" } },
  { $group: { _id: "$team", avg_a: { $avg: "$p.passes" } } },
  { $match: { avg_a: { $gt: 150 } } },
  { $sort: { avg_a: -1 } },
  { $project: { _id: 0, Team: "$_id", "Average Passes": "$avg_a" } }
]);
