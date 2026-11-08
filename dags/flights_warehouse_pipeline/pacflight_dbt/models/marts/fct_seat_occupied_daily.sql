WITH 
    stg_flights AS (
        SELECT * FROM {{ source('stg', 'flights') }}
    ),
    dim_dates AS (
        SELECT * FROM {{ ref('dim_date') }}
    ),
    dim_airports AS (
        SELECT * FROM {{ ref('dim_airport') }} WHERE dbt_valid_to IS NULL
    ),
    dim_aircrafts AS (
        SELECT * FROM {{ ref('dim_aircrafts') }} WHERE dbt_valid_to IS NULL
    ),
    stg_boarding_passes AS (
        SELECT * FROM {{ source('stg', 'boarding_passes') }}
    ),
    stg_seats AS (
        SELECT * FROM {{ source('stg', 'seats') }}
    ),
    
    cnt_seat_occupied AS (
        SELECT
            sf.flight_id,
            COUNT(seat_no) AS seat_occupied
        FROM {{ source('stg', 'flights') }} sf
        JOIN {{ source('stg', 'boarding_passes') }} sbp 
            ON sbp.flight_id = sf.flight_id
        WHERE sf.status = 'Arrived'
        GROUP BY 1
    ),
    
    cnt_total_seats AS (
        SELECT
            aircraft_code,
            COUNT(seat_no) AS total_seat
        FROM {{ source('stg', 'seats') }}
        GROUP BY 1
    ),
    
    final_fct_seat_occupied_daily AS (
        SELECT 
            dd.date_id AS date_flight,
            sf.flight_id AS flight_nk,
            sf.flight_no,
            da1.airport_id AS departure_airport,
            da2.airport_id AS arrival_airport,
            dac.aircraft_id AS aircraft_code,
            sf.status,
            cts.total_seat,
            cso.seat_occupied,
            (cts.total_seat - cso.seat_occupied) AS empty_seats
        FROM {{ source('stg', 'flights') }} sf
        JOIN {{ ref('dim_date') }} dd ON dd.date_actual = DATE(sf.actual_departure)
        JOIN {{ ref('dim_airport') }} da1 ON da1.airport_nk = sf.departure_airport AND da1.dbt_valid_to IS NULL
        JOIN {{ ref('dim_airport') }} da2 ON da2.airport_nk = sf.arrival_airport AND da2.dbt_valid_to IS NULL
        JOIN {{ ref('dim_aircrafts') }} dac ON dac.aircraft_nk = sf.aircraft_code AND dac.dbt_valid_to IS NULL
        JOIN cnt_seat_occupied cso ON cso.flight_id = sf.flight_id
        JOIN cnt_total_seats cts ON cts.aircraft_code = sf.aircraft_code
    )

SELECT * FROM final_fct_seat_occupied_daily
