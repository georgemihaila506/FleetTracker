"""Consumers — processes that read the position stream and do something with it.

Each is an independent subscriber (ADR-0003: separate concerns = separate
subscriptions, "groups fan out"). Added per milestone:

    materializer.py   maintains positions:current:{city} snapshot   (M4, ADR-0005)
    geofence.py       durable geofence-crossing alerts              (M7, ADR-0004)
    analytics.py      rolling per-vehicle stats                     (M8)
    dropout.py        manufactures "vehicle offline" events         (M9, ADR-0002)
"""
