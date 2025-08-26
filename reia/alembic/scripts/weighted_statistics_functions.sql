-- Weighted statistics functions for sparse data optimization
-- These functions handle sparse data where sum(weights) < 1.0 represents implicit zeros

-- Create weighted mean function for sparse data
CREATE OR REPLACE FUNCTION weighted_mean_sparse(
    vals DOUBLE PRECISION[],
    weights DOUBLE PRECISION[]
) RETURNS DOUBLE PRECISION AS $$
BEGIN
    -- Handle NULL or empty arrays
    IF vals IS NULL OR weights IS NULL OR array_length(vals, 1) IS NULL THEN
        RETURN 0.0;
    END IF;
    
    -- For sparse data: return sum(weight * value) directly
    -- This assumes the missing weight represents zero values
    RETURN (
        SELECT SUM(v * w)
        FROM unnest(vals, weights) AS t(v, w)
        WHERE w > 0
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

-- Create weighted quantile function for sparse data
CREATE OR REPLACE FUNCTION weighted_quantile_sparse(
    vals DOUBLE PRECISION[],
    weights DOUBLE PRECISION[],
    quantiles DOUBLE PRECISION[]
) RETURNS DOUBLE PRECISION[] AS $$
DECLARE
    sorted_indices INTEGER[];
    cumsum DOUBLE PRECISION;
    total_weight DOUBLE PRECISION;
    result DOUBLE PRECISION[];
    n INTEGER;
    idx INTEGER;
    jdx INTEGER;
    q DOUBLE PRECISION;
    prev_cumsum DOUBLE PRECISION;
    extended_vals DOUBLE PRECISION[];
    extended_weights DOUBLE PRECISION[];
    zero_weight DOUBLE PRECISION;
BEGIN
    -- Handle NULL or empty arrays
    IF vals IS NULL OR weights IS NULL OR array_length(vals, 1) IS NULL THEN
        RETURN array_fill(0.0, ARRAY[array_length(quantiles, 1)]);
    END IF;
    
    n := array_length(vals, 1);
    IF n = 0 THEN
        RETURN array_fill(0.0, ARRAY[array_length(quantiles, 1)]);
    END IF;
    
    -- Calculate total weight
    total_weight := 0.0;
    FOR idx IN 1..n LOOP
        IF weights[idx] > 0 THEN
            total_weight := total_weight + weights[idx];
        END IF;
    END LOOP;
    
    -- Handle sparse data: add zeros if sum of weights < 1
    IF total_weight < 1.0 THEN
        zero_weight := 1.0 - total_weight;
        extended_vals := vals || ARRAY[0.0];
        extended_weights := weights || ARRAY[zero_weight];
        n := n + 1;
    ELSE
        extended_vals := vals;
        extended_weights := weights;
    END IF;
    
    -- Get sorted indices for extended arrays
    sorted_indices := ARRAY(
        SELECT generate_series 
        FROM generate_series(1, n)
        ORDER BY extended_vals[generate_series]
    );
    
    -- Calculate percentiles using C=0 method with extended data
    result := ARRAY[]::DOUBLE PRECISION[];
    
    FOR idx IN 1..array_length(quantiles, 1) LOOP
        q := quantiles[idx];
        
        -- Handle edge cases
        IF q <= 0 THEN
            result := array_append(result, extended_vals[sorted_indices[1]]);
        ELSIF q >= 1 THEN
            result := array_append(result, extended_vals[sorted_indices[n]]);
        ELSE
            -- Find the position using cumulative sum
            cumsum := 0.0;
            prev_cumsum := 0.0;
            
            FOR jdx IN 1..n LOOP
                prev_cumsum := cumsum;
                cumsum := cumsum + extended_weights[sorted_indices[jdx]];
                
                IF cumsum >= q THEN
                    -- Linear interpolation
                    IF jdx > 1 AND cumsum > prev_cumsum THEN
                        result := array_append(result,
                            extended_vals[sorted_indices[jdx-1]] + 
                            (extended_vals[sorted_indices[jdx]] - extended_vals[sorted_indices[jdx-1]]) * 
                            ((q - prev_cumsum) / (cumsum - prev_cumsum))
                        );
                    ELSE
                        result := array_append(result, extended_vals[sorted_indices[jdx]]);
                    END IF;
                    EXIT;
                END IF;
            END LOOP;
            
            -- If we didn't find it (shouldn't happen), use last value
            IF array_length(result, 1) < idx THEN
                result := array_append(result, extended_vals[sorted_indices[n]]);
            END IF;
        END IF;
    END LOOP;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;