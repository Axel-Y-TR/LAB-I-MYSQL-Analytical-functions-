USE sakila;

SELECT customer_id, payment_date, amount,
SUM(amount) OVER (PARTITION BY customer_id ORDER BY payment_date) AS running_total
FROM payment
ORDER BY customer_id, payment_date;

SELECT DATE(payment_date) AS payment_date, amount,
RANK() OVER (PARTITION BY DATE(payment_date) ORDER BY amount DESC) AS rk,
DENSE_RANK() OVER (PARTITION BY DATE(payment_date) ORDER BY amount DESC) AS dk
FROM payment;

SELECT film.rental_rate, film.title, category.name ,
RANK() OVER (PARTITION BY category.category_id ORDER BY film.rental_rate) AS rk,
DENSE_RANK() OVER (PARTITION BY category.category_id ORDER BY film.rental_rate) AS dk
from film
join film_category on film.film_id = film_category.film_id
join category on film_category.category_id = category.category_id;


with Ranked as (
SELECT film.rental_rate, film.title, category.name ,
RANK() OVER (PARTITION BY category.category_id ORDER BY film.rental_rate desc ) AS rk,
DENSE_RANK() OVER (PARTITION BY category.category_id ORDER BY film.rental_rate Desc) AS dk,
ROW_NUMBER() OVER (PARTITION BY category.category_id ORDER BY film.rental_rate DESC ) AS rn
from film
join film_category on film.film_id = film_category.film_id
join category on film_category.category_id = category.category_id)

select * from Ranked where rn <=5;



SELECT payment_id, customer_id, amount, payment_date,
amount - LAG(amount) OVER (PARTITION BY customer_id ORDER BY payment_date) AS diff_previous,
LEAD(amount) OVER (PARTITION BY customer_id ORDER BY payment_date) - amount AS diff_next
FROM payment;






