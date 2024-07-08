SELECT id1, id2, id3, final_var
FROM (
    SELECT first(id1) as id1, first(id2) as id2, first(id3) as id3, sorted_id, 
            SUM(var) + 2/10000*SUM(dot) - 2*SUM(mean_product) AS final_var
    FROM(
        SELECT t2.id1 as id1, t2.id2 as id2, t1.id as id3, t2.mean_product as mean_product, t1.var as var, t2.dot as dot,
            concat_ws('', array_sort(array(t2.id1, t2.id2, t1.id))) as sorted_id
        FROM (
            SELECT data.id as id,
                aggregate(data.int_list, cast(0 as long), (acc, x) -> acc + x*x)/size(data.int_list) - POW(aggregate(data.int_list, 0, (acc, x) -> acc + x)/size(data.int_list), 2) as var
            FROM data 
            -- calculates the variance var for each row in the data table
            ) as t1
        CROSS JOIN (
            SELECT data1.id as id1, data2.id as id2,
                aggregate(zip_with(data1.int_list, data2.int_list, (x, y) -> x * y), 0, (acc, x) -> acc + x) as dot,
                aggregate(data1.int_list, 0, (acc, x) -> acc + x)/size(data1.int_list) * aggregate(data2.int_list, 0, (acc, x) -> acc + x)/size(data2.int_list) as mean_product
            FROM data as data1, data as data2
            WHERE data1.id < data2.id 
            -- dot product and mean product for each pair of rows in the data table
            ) as t2
        WHERE t1.id!= t2.id1 and t1.id!=t2.id2 
        -- joins two subqueries (t1 and t2) using a cross join 
        -- and applies a filter to exclude rows 
        -- where t1.id matches either t2.id1 or t2.id2
        -- and then  constructs a sorted_id by 
        -- concatenating and sorting the id1, id2, and id3 values
        )
    GROUP BY sorted_id )
    -- aggregates the results by sorted_id
WHERE final_var <= tau
-- final_var that is no greater than tau

