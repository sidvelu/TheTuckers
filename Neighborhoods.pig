REGISTER elephant-bird-2.2.3.jar

all_records = LOAD 'yelp_academic_dataset_business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

filtered_records = FILTER all_records BY json#'stars' >= 4.0;

b = FOREACH filtered_records GENERATE FLATTEN(json#'neighborhoods');

neighborhoods = DISTINCT b;

-- Allows to order by stars (doesn't work ordering by #'d field)
a_with_stars = foreach filtered_records generate json as (json:map[]), json#'stars' as stars, json#'stars' * json#'review_count' as factor;

grouped_records = GROUP a_with_stars BY json#'neighborhoods';

top_five = FOREACH grouped_records {
	sorted = ORDER a_with_stars BY factor desc;
	top = LIMIT sorted 5;
	GENERATE group, FLATTEN(top);
};

top_five_small = FOREACH top_five generate json#'neighborhoods' as neighborhood, factor as factor, json#'stars' as stars, json#'name' as name;

STORE top_five_small INTO 'output';

