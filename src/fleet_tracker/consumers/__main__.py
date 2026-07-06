"""`python -m fleet_tracker.consumers` runs the materializer (the M4 consumer).

As more consumers land (geofence, analytics, dropout), this can grow into a small
dispatcher; for now it's just the materializer.
"""

from .materializer import main

main()
