// MongoDB Playground 
// Run queries
// - select code lines : db & query 
// - left-click on right arrow icon on top bar
// To view sample Docs
// in left-bar, right-click collection "View Documents" 

// Query #1
use('db_datalake'); // DB selection
db.getCollection('reviews').aggregate(
  { 
    $group: {_id: null, // null means "group everything into one bucket"
      maxReviewTime: { $max: "$reviewTime" },
      minReviewTime: { $min: "$reviewTime" }
    }
  }
);

/* Results #1
{
  _id: null,
  maxReviewTime: '12 9, 2017',
  minReviewTime: '01 1, 2000'
}
*/
