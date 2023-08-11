-- SQL-команды для создания таблиц
CREATE TABLE IF NOT EXISTS employees
(
	employee_id int PRIMARY KEY,
 	first_name varchar(100) NOT NULL,
 	last_name varchar(100)NOT NULL,
 	title varchar(100)NOT NULL,
 	birth_date date,
	notes text
);

CREATE TABLE IF NOT EXISTS customers
(
	customer_id varchar(100) PRIMARY KEY,
	company_name varchar(100) NOT NULL,
	contact_name varchar(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders
(
	order_id int PRIMARY KEY,
	customer_id varchar(100) REFERENCES customers(customer_id),
	employee_id int REFERENCES employees(employee_id),
	order_date date,
	ship_city varchar(100) NOT NULL
);
