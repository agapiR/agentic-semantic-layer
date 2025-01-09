-- Create the tables
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100),
    addr VARCHAR(100),
    phone VARCHAR(20),
    crd_lmt DECIMAL(10, 2),
    birth_dt DATE,
    email_verified BOOLEAN,
    phone_verified BOOLEAN,
    rank INT,
    registr_dt DATE
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    cu_id INT,
    order_dt DATE,
    tot_paid DECIMAL(10, 2),
    status VARCHAR(20),
    tracking_no VARCHAR(50),
    transp_co VARCHAR(50),
    itm_cnt INT,
    off_code VARCHAR(20),
    FOREIGN KEY (cu_id) REFERENCES customers(id)
);

CREATE TABLE suppliers (
    id INT PRIMARY KEY,
    nm VARCHAR(50),
    contact_nm VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    addr VARCHAR(100)
);

CREATE TABLE products (
    id INT PRIMARY KEY,
    nm VARCHAR(50),
    price DECIMAL(10, 2),
    cat VARCHAR(50),
    su_id INT,
    FOREIGN KEY (su_id) REFERENCES suppliers(id)
);

CREATE TABLE order_items (
    id INT PRIMARY KEY,
    o_id INT,
    p_id INT,
    quant INT,
    price_paid DECIMAL(10, 2),
    disc_amt DECIMAL(10, 2),
    reshipped BOOLEAN,
    return_policy VARCHAR(50),
    FOREIGN KEY (o_id) REFERENCES orders(id),
    FOREIGN KEY (p_id) REFERENCES products(id)
);


-- Insert some data into the tables, 10 rows for each table

-- Inserting sample data into customers table
INSERT INTO customers (id, name, email, addr, phone, crd_lmt, birth_dt, email_verified, phone_verified, rank, registr_dt) VALUES
(1, 'John Doe', 'johndoe@example.com', '123 Elm St', '555-1234', 1000.00, '1985-01-15', TRUE, TRUE, 5, '2020-05-01'),
(2, 'Jane Smith', 'janesmith@example.com', '456 Maple St', '555-5678', 1500.00, '1990-03-22', TRUE, FALSE, 4, '2021-08-15'),
(3, 'Alice Johnson', 'alicej@example.com', '789 Oak St', '555-8765', 2000.00, '1982-06-10', FALSE, TRUE, 3, '2019-12-11'),
(4, 'Bob Brown', 'bob_brown@example.com', '321 Pine St', '555-4321', 1200.00, '1975-11-30', TRUE, TRUE, 5, '2018-07-23'),
(5, 'Carol White', 'carolwhite@example.com', '654 Cedar St', '555-6789', 1100.00, '1995-02-18', FALSE, FALSE, 2, '2022-02-05'),
(6, 'David Black', 'davidblack@example.com', '987 Birch St', '555-9876', 1700.00, '1980-08-19', TRUE, TRUE, 4, '2017-10-30'),
(7, 'Eve Green', 'evegreen@example.com', '159 Ash St', '555-2468', 1300.00, '1992-12-25', TRUE, TRUE, 3, '2023-03-15'),
(8, 'Frank Blue', 'frankblue@example.com', '357 Fir St', '555-1357', 900.00, '1988-07-04', FALSE, TRUE, 5, '2021-11-09'),
(9, 'Grace Yellow', 'graceyellow@example.com', '753 Palm St', '555-8642', 1400.00, '1978-09-17', TRUE, FALSE, 2, '2019-06-20'),
(10, 'Henry Purple', 'henrypurple@example.com', '951 Spruce St', '555-9753', 1600.00, '1996-04-02', FALSE, TRUE, 4, '2020-01-27');

-- Inserting sample data into orders table
INSERT INTO orders (id, cu_id, order_dt, tot_paid, status, tracking_no, transp_co, itm_cnt, off_code) VALUES
(1, 1, '2023-01-10', 250.00, 'shipped', 'TRK123456', 'FedEx', 3, 'DIS10'),
(2, 2, '2023-01-15', 150.00, 'delivered', 'TRK234567', 'UPS', 2, 'OFF20'),
(3, 3, '2023-02-05', 300.00, 'pending', 'TRK345678', 'DHL', 5, 'None'),
(4, 4, '2023-03-12', 400.00, 'delivered', 'TRK456789', 'FedEx', 4, 'OFF10'),
(5, 5, '2023-03-20', 100.00, 'canceled', 'TRK567890', 'UPS', 1, 'DIS20'),
(6, 6, '2023-04-02', 500.00, 'shipped', 'TRK678901', 'DHL', 6, 'OFF10'),
(7, 7, '2023-05-09', 600.00, 'returned', 'TRK789012', 'FedEx', 7, 'None'),
(8, 8, '2023-05-17', 350.00, 'delivered', 'TRK890123', 'UPS', 4, 'OFF20'),
(9, 9, '2023-06-25', 450.00, 'shipped', 'TRK901234', 'DHL', 3, 'DIS20'),
(10, 10, '2023-07-01', 200.00, 'pending', 'TRK012345', 'FedEx', 2, 'None');

-- Inserting sample data into products table
INSERT INTO products (id, nm, price, cat, su_id) VALUES
(1, 'Laptop', 1200.00, 'Electronics', 1),
(2, 'Smartphone', 800.00, 'Electronics', 2),
(3, 'Headphones', 150.00, 'Accessories', 3),
(4, 'Monitor', 300.00, 'Electronics', 4),
(5, 'Keyboard', 50.00, 'Accessories', 5),
(6, 'Mouse', 30.00, 'Accessories', 2),
(7, 'Desk Chair', 250.00, 'Furniture', 3),
(8, 'Desk Lamp', 40.00, 'Furniture', 1),
(9, 'External HDD', 100.00, 'Electronics', 4),
(10, 'USB Cable', 10.00, 'Accessories', 5);

-- Inserting sample data into order_items table
INSERT INTO order_items (id, o_id, p_id, quant, price_paid, disc_amt, reshipped, return_policy) VALUES
(1, 1, 1, 1, 1200.00, 50.00, FALSE, '30-day'),
(2, 2, 2, 1, 800.00, 25.00, FALSE, '14-day'),
(3, 3, 3, 2, 150.00, 0.00, FALSE, 'no-return'),
(4, 4, 4, 1, 300.00, 20.00, FALSE, '14-day'),
(5, 5, 5, 3, 50.00, 10.00, TRUE, '30-day'),
(6, 6, 6, 1, 30.00, 5.00, FALSE, 'no-return'),
(7, 7, 7, 2, 250.00, 0.00, TRUE, '30-day'),
(8, 8, 8, 1, 40.00, 5.00, FALSE, '14-day'),
(9, 9, 9, 3, 100.00, 10.00, FALSE, '30-day'),
(10, 10, 10, 2, 10.00, 0.00, TRUE, '14-day');

-- Inserting sample data into suppliers table
INSERT INTO suppliers (id, nm, contact_nm, email, phone, addr) VALUES
(1, 'Tech Supplies Co.', 'Sam Wilson', 'samwilson@techsupplies.com', '555-1111', '100 Supply St'),
(2, 'Gadget World', 'Tom Hanks', 'tom.hanks@gadgetworld.com', '555-2222', '200 Gadget Ave'),
(3, 'Audio Shop', 'Kelly Clarkson', 'kelly@audioshop.com', '555-3333', '300 Audio Ln'),
(4, 'Vision Tech', 'James Cameron', 'james@visiontech.com', '555-4444', '400 Vision Blvd'),
(5, 'Accessories R Us', 'Anna Bell', 'anna.bell@accessoriesrus.com', '555-5555', '500 Accessory Ct'),
(6, 'Furniture Mart', 'Bob Ross', 'bob.ross@mail.com', '555-6666', '600 Furniture Rd'),
(7, 'Home Decor Co.', 'Sara Johnson', 'saraa@home.decor ', '555-7777', '700 Home Dr');
