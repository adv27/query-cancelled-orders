# Querying orders by their latest status


## Topic
### Read the topic [here](./TOPIC.md)


## Solution

### Defining the models
```python
# order/models.py

from django.db import models


class Order(models.Model):
    pass


class OrderStatus(models.Model):
    class Status(models.TextChoices):
        PENDING = 'Pending'
        COMPLETE = 'Complete'
        CANCELLED = 'Cancelled'

    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name="statuses")
    status = models.CharField(choices=Status)
    created = models.DateTimeField(auto_now_add=True)
```

### Seeding the orders
```python
# order/management/commands/seed_orders.py

import random
from itertools import islice

from django.core.management.base import BaseCommand

from twitter_sentiment_price.order.models import OrderStatus, Order


class Command(BaseCommand):
    help = "Seeding orders"

    def add_arguments(self, parser):
        parser.add_argument("total", metavar="N", type=int, help="Number of orders to generate")

    def handle(self, *args, **options):
        statuses_choices: list[list[OrderStatus.Status]] = [
            [OrderStatus.Status.PENDING, OrderStatus.Status.COMPLETE],  # Successful payment
            [OrderStatus.Status.PENDING, OrderStatus.Status.CANCELLED],  # Payment fails
            [OrderStatus.Status.PENDING, OrderStatus.Status.COMPLETE, OrderStatus.Status.CANCELLED],  # Refunded
        ]

        total = options["total"]
        batch_size = 50_000
        orders = (Order() for _ in range(total))
        while True:
            batch = list(islice(orders, batch_size))
            if not batch:
                break

            created_orders = Order.objects.bulk_create(batch, batch_size)
            order_statuses = []
            for o in created_orders:
                statuses = random.choice(statuses_choices)
                order_statuses.extend([OrderStatus(order=o, status=status) for status in statuses])
            OrderStatus.objects.bulk_create(order_statuses)

        self.stdout.write(self.style.SUCCESS(f"Successfully created {total} orders"))
```
Let's generate `1,000,000` orders
```bash
$ python manage.py seed_orders 1000000
```
Go take a coffee ☕, this might take a while.

### Implement query

#### Notes  
 - My database contains around `1,200,000` Order and  `2,700,000` OrderStatuses

#### Simplest version
Every `Cancelled` order will always has one `OrderStatus` with `status="Cancelled"`.  
So the query for listing all `Cancelled` orders will be:  
 - Django filter:
    ```python
    cancelled_orders = Order.objects.filter(statuses__status=OrderStatus.Status.CANCELLED)
    ```
 - Executed SQL:
    ```postgresql
    SELECT "order_order"."id"
      FROM "order_order"
     INNER JOIN "order_orderstatus"
        ON ("order_order"."id" = "order_orderstatus"."order_id")
     WHERE "order_orderstatus"."status" = '''Cancelled'''
    ```
 - Time:
    ```text
    709.75 ms
    ```
But this answer seem fairly simple, I think the question should be `filter orders based on their latest status`

#### `ROW_NUMBER()` version
The intent is doing filter on the latest status only:
 - Number the rows in `OrderStatus` table so that the most recent status of `Order` get number **1** (after sort `DESC` created).  
 - Select that only most recent row.  
 - Join with the `Order` table to containing only the most recent `OrderStatus` matching.  

So the query for listing all `Cancelled` orders will be:  
 - Django filter:
    ```python
    from django.db.models import F
    from django.db.models import Subquery, OuterRef
    from django.db.models.expressions import Window
    from django.db.models.functions import RowNumber
   
    latest_status = OrderStatus.objects.annotate(
        row_number=Window(expression=RowNumber(), partition_by=[F("order")], order_by=[F("created").desc()])
    ).filter(
        row_number=1,
        order_id=OuterRef("pk"),
    )
    cancelled_orders = Order.objects.alias(latest_status=Subquery(latest_status.values("status")[:1])).filter(
        latest_status=OrderStatus.Status.CANCELLED
    )
    ```
 - Executed SQL:
    ```postgresql
    SELECT "order_order"."id"
      FROM "order_order"
     WHERE (
            SELECT "col1"
              FROM (
                    SELECT *
                      FROM (
                            SELECT U0."status" AS "col1",
                                   ROW_NUMBER() OVER (PARTITION BY U0."order_id" ORDER BY U0."created" DESC) AS "qual0"
                              FROM "order_orderstatus" U0
                             WHERE U0."order_id" = ("order_order"."id")
                           ) "qualify"
                     WHERE "qual0" = 'Int4(1)'
                   ) "qualify_mask"
             LIMIT 1
           ) = '''Cancelled'''
    ```
 - Time:
    ```text
    3880.65 ms
    ```
 - Query plan:
    ```text
    Seq Scan on order_order  (cost=0.00..10056201.63 rows=5876 width=8) (actual time=117.814..4037.723 rows=770965 loops=1)
      Filter: (((SubPlan 1))::text = 'Cancelled'::text)
      Rows Removed by Filter: 404247
      SubPlan 1
        ->  Limit  (cost=8.47..8.54 rows=1 width=8) (actual time=0.003..0.003 rows=1 loops=1175212)
              ->  Subquery Scan on qualify  (cost=8.47..8.54 rows=1 width=8) (actual time=0.003..0.003 rows=1 loops=1175212)
                    Filter: (qualify.qual0 = 1)
                    ->  WindowAgg  (cost=8.47..8.52 rows=2 width=32) (actual time=0.003..0.003 rows=1 loops=1175212)
                          Run Condition: (row_number() OVER (?) <= 1)
                          ->  Sort  (cost=8.47..8.48 rows=2 width=24) (actual time=0.002..0.002 rows=2 loops=1175212)
                                Sort Key: u0.created DESC
                                Sort Method: quicksort  Memory: 25kB
                                ->  Index Scan using order_orderstatus_order_id_d13e4d24 on order_orderstatus u0  (cost=0.43..8.46 rows=2 width=24) (actual time=0.001..0.001 rows=2 loops=1175212)
                                      Index Cond: (order_id = order_order.id)
    Planning Time: 0.374 ms
    JIT:
      Functions: 17
      Options: Inlining true, Optimization true, Expressions true, Deforming true
      Timing: Generation 0.587 ms, Inlining 46.927 ms, Optimization 39.429 ms, Emission 31.280 ms, Total 118.223 ms
    Execution Time: 4082.676 ms
    ```

#### Nested sub-query version
Using `Subquery` then get the only **first** row  

So the query for listing all `Cancelled` orders will be:  
 - Django filter:
    ```python
    from django.db.models import Subquery, OuterRef
   
    latest_status = OrderStatus.objects.filter(
        order_id=OuterRef("pk")
    ).order_by(
        "-created"
    )
    cancelled_orders = Order.objects.alias(
        latest_status=Subquery(latest_status.values("status")[:1])
    ).filter(
        latest_status=OrderStatus.Status.CANCELLED
    )
    ```
 - Executed SQL:
    ```postgresql
   SELECT "order_order"."id"
     FROM "order_order"
    WHERE (
           SELECT U0."status"
             FROM "order_orderstatus" U0
            WHERE U0."order_id" = ("order_order"."id")
            ORDER BY U0."created" DESC
            LIMIT 1
          ) = '''Cancelled'''
    ```
 - Time:
    ```text
    3185.36 ms 
    ```
 - Query plan:
    ```text
   Seq Scan on order_order  (cost=0.00..9982750.88 rows=5876 width=8) (actual time=68.345..2945.670 rows=770965 loops=1)
     Filter: (((SubPlan 1))::text = 'Cancelled'::text)
     Rows Removed by Filter: 404247
     SubPlan 1
       ->  Limit  (cost=8.47..8.48 rows=1 width=16) (actual time=0.002..0.002 rows=1 loops=1175212)
             ->  Sort  (cost=8.47..8.48 rows=2 width=16) (actual time=0.002..0.002 rows=1 loops=1175212)
                   Sort Key: u0.created DESC
                   Sort Method: quicksort  Memory: 25kB
                   ->  Index Scan using order_orderstatus_order_id_d13e4d24 on order_orderstatus u0  (cost=0.43..8.46 rows=2 width=16) (actual time=0.001..0.001 rows=2 loops=1175212)
                         Index Cond: (order_id = order_order.id)
   Planning Time: 0.369 ms
   JIT:
     Functions: 10
     Options: Inlining true, Optimization true, Expressions true, Deforming true
     Timing: Generation 0.290 ms, Inlining 44.979 ms, Optimization 13.651 ms, Emission 9.584 ms, Total 68.504 ms
   Execution Time: 2979.159 ms
    ```









