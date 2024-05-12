// Task 2ii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    // first split every tagline to array of substrings
    {$project: {
        subStrArray: {$split: [
            "$tagline", " "
        ]}
    }},
    // use $unwind to deconstructs this array field from the input documents to output a document for each element.
    {$unwind: 
        "$subStrArray"
    },
    // use project to rename the new split subStr element
    {$project: {
        subStr: "$subStrArray"
    }},
    // trim off surrounding punctuation marks and their len
    {$project: {
        subStr: {$trim: {
            input: "$subStr",
            chars: ".,!?"
        }},
        len: {$strLenCP: "$subStr"}
    }},
    // find len > 3
    {$match: {
        len: {$gt: 3}
    }},
    // converts strings to lowercase
    {$project: {
        lower_subStr: {$toLower: 
            "$subStr"
        }
    }},
    // group by subStr and do count
    {$group: {
        _id: "$lower_subStr",
        count: {$sum: 1}
    }},
    // sorted by count DESC
    {$sort: {
        count: -1
    }},
    // output _id and count
    {$project: {
        _id: 1,
        count: 1
    }},
    // limit 20
    {$limit: 
        20
    }
]);