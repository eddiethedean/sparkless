"""StorageLevel constants for PySpark compatibility.

Provides storage level constants that match PySpark's StorageLevel class.
In sparkless these are simple string constants since actual storage tiers
are not implemented.
"""


class StorageLevel:
    """Mock StorageLevel matching PySpark's storage level constants."""

    DISK_ONLY = "DISK_ONLY"
    DISK_ONLY_2 = "DISK_ONLY_2"
    DISK_ONLY_3 = "DISK_ONLY_3"
    MEMORY_AND_DISK = "MEMORY_AND_DISK"
    MEMORY_AND_DISK_2 = "MEMORY_AND_DISK_2"
    MEMORY_AND_DISK_DESER = "MEMORY_AND_DISK_DESER"
    MEMORY_ONLY = "MEMORY_ONLY"
    MEMORY_ONLY_2 = "MEMORY_ONLY_2"
    MEMORY_ONLY_SER = "MEMORY_ONLY_SER"
    MEMORY_ONLY_SER_2 = "MEMORY_ONLY_SER_2"
    OFF_HEAP = "OFF_HEAP"
    NONE = "NONE"
