REGISTER '../elephant-bird-2.2.3.jar'

all_records = LOAD 'reviews.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);


STORE all_records INTO 'output';
