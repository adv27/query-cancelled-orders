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
 - My database contains around `1,200,000` Orders and  `2,700,000` OrderStatuses

#### Simplest version
Since no status can come after `Cancelled`, so it will always be the last one in list of `OrderStatus` related objects.
So the query for listing all `Cancelled` orders will be:  
 - Django filter:
    ```python
    cancelled_orders = Order.objects.filter(statuses__status=OrderStatus.Status.CANCELLED)
    ```
 - Executed SQL:
    ```sql
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
 - Query plan
    ```text
    Hash Join  (cost=36234.27..104050.16 rows=771854 width=8) (actual time=227.303..744.650 rows=770966 loops=1)
      Hash Cond: (order_orderstatus.order_id = order_order.id)
      ->  Seq Scan on order_orderstatus  (cost=0.00..55166.76 rows=771854 width=8) (actual time=4.215..218.972 rows=770966 loops=1)
            Filter: ((status)::text = 'Cancelled'::text)
            Rows Removed by Filter: 1928455
      ->  Hash  (cost=16953.12..16953.12 rows=1175212 width=8) (actual time=222.500..222.501 rows=1175212 loops=1)
            Buckets: 262144  Batches: 8  Memory Usage: 7796kB
            ->  Seq Scan on order_order  (cost=0.00..16953.12 rows=1175212 width=8) (actual time=0.034..77.977 rows=1175212 loops=1)
    Planning Time: 0.389 ms
    JIT:
      Functions: 12
      Options: Inlining false, Optimization false, Expressions true, Deforming true
      Timing: Generation 0.386 ms, Inlining 0.000 ms, Optimization 0.188 ms, Emission 4.014 ms, Total 4.588 ms
    Execution Time: 777.449 ms
   ```

Lets add index on `OrderStatus.status`
```python
class OrderStatus(models.Model):
    class Status(models.TextChoices):
        PENDING = "Pending"
        COMPLETE = "Complete"
        CANCELLED = "Cancelled"

    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name="statuses")
    status = models.CharField(choices=Status.choices)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [models.Index(fields=["status"])]
```
After the migration for new index added, the query plan will be:
```text
Hash Join  (cost=44860.57..88581.87 rows=771854 width=8) (actual time=251.315..719.570 rows=770966 loops=1)
  Hash Cond: (order_orderstatus.order_id = order_order.id)
  ->  Bitmap Heap Scan on order_orderstatus  (cost=8626.30..39698.47 rows=771854 width=8) (actual time=18.748..179.598 rows=770966 loops=1)
        Recheck Cond: ((status)::text = 'Cancelled'::text)
        Heap Blocks: exact=21424
        ->  Bitmap Index Scan on order_order_status_288d9a_idx  (cost=0.00..8433.33 rows=771854 width=0) (actual time=16.099..16.100 rows=770966 loops=1)
              Index Cond: ((status)::text = 'Cancelled'::text)
  ->  Hash  (cost=16953.12..16953.12 rows=1175212 width=8) (actual time=231.947..231.947 rows=1175212 loops=1)
        Buckets: 262144  Batches: 8  Memory Usage: 7796kB
        ->  Seq Scan on order_order  (cost=0.00..16953.12 rows=1175212 width=8) (actual time=0.013..83.136 rows=1175212 loops=1)
Planning Time: 0.466 ms
Execution Time: 740.313 ms
```
We can see `Seq Scan on order_orderstatus` changed into `Bitmap Heap Scan on order_orderstatus` since now we're using index for filtering,
it means that only some pages need to be read rather than all of them.


### Optimization Suggestions
 - Indexing:  
   Create indexes on:
     - `OrderStatus.status` 
     - `OrderStatus.created` 
     - `OrderStatus.order_id` (Django already done this behind the scene)  
     for faster filtering and joining.
 - Denormalization: 
   - Consider storing the latest status directly on the `Order` model for faster retrieval, 
   especially if querying for the latest status is frequent.
 - Caching

### Schema changes examples
- Storing the latest status directly on the `Order` model
    ```python
    class Order(models.Model):
        status = models.CharField(max_length=12)

        class Meta:
            indexes = [models.Index(fields=["status"])]
    ```   

    ```python
    # order/signals.py
    
    @receiver(post_save, sender=OrderStatus)
    def update_order_status(sender, instance, created, **kwargs):
        if created:
            order = instance.order
            order.status = instance.status
            order.save(update_fields=['status'])
    ```