-- INITIAL TABLE
CREATE TABLE IF NOT EXISTS initial_price (
    id INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    commodity_name VARCHAR(150) NOT NULL,
    market_name VARCHAR(255) NOT NULL,
    minimum_price DECIMAL(9, 2) DEFAULT NULL,
    maximum_price DECIMAL(9, 2) DEFAULT NULL,
    mean_price DECIMAL(9, 2) DEFAULT NULL,
    date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS retrieved_files (
    id INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    date DATE NOT NULL,
    is_success BOOLEAN DEFAULT TRUE,
    date_added DATE DEFAULT CURRENT_DATE
);

-- -- Commodity Table
-- CREATE TABLE IF NOT EXISTS commodity (
--     id INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
--     name VARCHAR(255) NOT NULL,
--     subvariant VARCHAR(255) DEFAULT NULL
-- );

-- -- Market Table
-- CREATE TABLE IF NOT EXISTS market (
--     id INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
--     name VARCHAR(255) NOT NULL,
--     city VARCHAR(255)
-- );

-- -- Price Table
-- CREATE TABLE IF NOT EXISTS price (
--     id INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
--     commodity_id INT NOT NULL,
--     market_id INT NOT NULL,
--     minimum_price DECIMAL(9, 2),
--     maximum_price DECIMAL(9, 2),
--     mean_price DECIMAL(9, 2),
--     is_available BOOLEAN DEFAULT FALSE,
--     date DATE
-- );

-- -- SEED
-- SELECT
--     *
-- FROM
--     commodity;

-- INSERT INTO
--     commodity (name)
-- VALUES
--     ('Well-milled Rice (Local)'),
--     ('Corn (White)'),
--     ('Corn (Yellow)'),
--     ('Tilapia'),
--     ('Galunggong'),
--     ('Egg (Medium)'),
--     ('Ampalalya'),
--     ('Tomato'),
--     ('Cabbage (Rareball)'),
--     ('Cabbage (Scorpio)'),
--     ('Cabbage (Wonderball)'),
--     ('Pechay Baguio'),
--     ('Red Onion (Local)'),
--     ('Sugar (Washed)'),
--     ('Fresh Pork Kasim/Pigue'),
--     ('Frozen Pork Kasim/Pigue'),
--     ('Fresh Pork Liempo'),
--     ('Frozen Pork Liempo'),
--     ('Fresh Whole Chicken');

-- SELECT
--     *
-- FROM
--     market;

-- INSERT INTO
--     market (name)
-- VALUES
--     ('Agora Public Market/San Juan'),
--     ('Balintawak (Cloverleaf) Market'),
--     ('Bicutan Market'),
--     ('Blumentritt Market'),
--     ('Cartimar Market'),
--     ('Commonwealth Market/Quezon City'),
--     ('Dagonoy Market'),
--     ('Guadalupe Public Market/Makati'),
--     ('Kamuning Public Market'),
--     ('La Huerta Market/Parañaque'),
--     ('New Las Piñas City Public Market'),
--     ('Malabon Central Market'),
--     ('Mandaluyong Public Market'),
--     ('Marikina Public Market'),
--     ('Maypajo Public Market/Caloocan'),
--     ('Mega Q-mart/Quezon City'),
--     ('Muñoz Market/Quezon City'),
--     ('Murphy Public Market'),
--     ('Navotas Agora Market'),
--     ('New Marulas Public Market/Valenzuela'),
--     ('Paco Market'),
--     ('Pasay City Market'),
--     ('Pateros Market'),
--     ('Pritil Market/Manila'),
--     ('Quinta Market/Manila'),
--     ('San Andres Market/Manila'),
--     ('Taguig People''s Market'),
--     ('Trabajo Market');
