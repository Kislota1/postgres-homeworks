-- SQL-команды для создания таблиц
CREATE TABLE employees
(
	employee_id int PRIMARY KEY,
	first_name varchar(20) NOT NULL,
	last_name varchar(20) NOT NULL,
	title varchar(50) NOT NULL,
	birth_date varchar(10) NOT NULL,
	notes text NOT NULL
);

CREATE TABLE customers
(
    customer_id varchar(5) PRIMARY KEY,
    company_name varchar(60) NOT NULL,
    contact_name varchar(50) NOT NULL
);

CREATE TABLE orders
(
    order_id int PRIMARY KEY,
    customer_id varchar(5) NOT NULL REFERENCES customers(customer_id),
    employee_id int NOT NULL REFERENCES employees(employee_id),
    order_date varchar(10) NOT NULL,
    ship_city varchar(30) NOT NULL
);