/* 
Goal : Shift forward all dates of the dataset by a fixed jump
Context: 
- The app processes only the current day's data every day.
- The original dataset can't provide data to the app because it's outdated:
    - maxReviewTime: '12 9, 2017'
    - minReviewTime: '01 1, 2000'
Solution:
- Shift forward to make the data appear current.
    - target date = today + 6 months
    - jump = target date - (current max date)
- batch size: for batch processing to avoid memory issues
- Update Flag: is set on updated records to avoid reprocessing
*/

// DB selection
use('db_datalake');

// Get Target Max Date
const targetMax = new Date(); // Today
targetMax.setMonth(targetMax.getMonth() + 6); // Target = Today + 6 Months

// Get dates range BEFORE update
const oldStats = db.reviews.aggregate(    
    { 
        $match: { migration_v1: { $exists: false } }  // avoid reprocessing
    },
    { 
        $group: {_id: null, // null means "group everything into one bucket"
            maxReviewTime: { $max: "$reviewTime" },
            minReviewTime: { $min: "$reviewTime" }
        }
    }
).toArray();
if (oldStats.length > 0) {
    const oldMin = new Date(oldStats[0].minReviewTime);
    console.log(`Old Min reviewTime: ${oldMin}`);
    const oldMax = new Date(oldStats[0].maxReviewTime);
    console.log(`Old Max reviewTime: ${oldMax}`);
    
    // Get Jump in ms and sec 
    const jumpMs = (targetMax.getTime() - oldMax.getTime()).toString();
    const jumpSec = (Math.floor(jumpMs / 1000)).toString(); // warning if no string
    console.log(`Advancing records by ${jumpMs}ms to match target: ${targetMax}`);

    // Set Batch Size
    const batchSize = 10000;
    
    // Set Cursor: omit flagged records
    const cursor = db.reviews.find({ migration_v1: { $exists: false } });

    // Loop records
    documents_updated = 0;
    while (cursor.hasNext()) {

        // Get batch of IDs
        const batch = [];
        for (let i = 0; i < batchSize && cursor.hasNext(); i++) {
            batch.push(cursor.next()._id);
        }

        try {
            // Update & flag records
            db.reviews.updateMany(
                { _id: { $in: batch } },
                [
                    {   // Convert string to Date object
                        $set: {
                            reviewTime: { 
                                $dateFromString: { 
                                    dateString: "$reviewTime", 
                                    format: "%m %d, %Y" 
                                } 
                            }
                        }
                    },
                    {   // Main Update
                        $set: {
                            reviewTime: { 
                                $add: [ 
                                    "$reviewTime",
                                    NumberLong(jumpMs) 
                                ] 
                            },
                            unixReviewTime: { 
                                $add: [
                                    "$unixReviewTime", 
                                    NumberLong(jumpSec)
                                ] 
                            },
                            migration_v1: true // Flag 
                        }
                    }
                ]
            );
            documents_updated += batch.length;
            console.log(`Updated ${documents_updated} records so far`);            
        } 
        catch (e) {
            console.error(`Error updating batch: ${e}`);
        }
    }
    console.log("Migration complete.");

} else {
    console.log("No records left to update.");
}

// Get dates range AFTER update
const newStats = db.reviews.aggregate(    
    { 
        $match: { migration_v1: { $exists: true } }  // filter in flagged records
    },
    { 
        $group: {_id: null, // null means "group everything into one bucket"
            maxReviewTime: { $max: "$reviewTime" },
            minReviewTime: { $min: "$reviewTime" }
        }
    }
).toArray();
if (newStats.length > 0) {
    const newMin = new Date(newStats[0].minReviewTime);
    console.log(`New Min reviewTime: ${newMin}`);
    const newMax = new Date(newStats[0].maxReviewTime);
    console.log(`New Max reviewTime: ${newMax}`);
}