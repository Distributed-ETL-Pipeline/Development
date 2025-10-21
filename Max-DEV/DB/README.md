## File Structure

```
your_project/
├── gtfs_processor.py      # Main processor class
├── gtfs_manager.py        # Interactive manager
├── requirements.txt       # Dependencies
├── README.md             # This file
└── GTFS/                 # Your GTFS data folder
    ├── agency.txt
    ├── routes.txt
    ├── stops.txt
    ├── trips.txt
    ├── stop_times.txt
    ├── calendar.txt
    ├── calendar_dates.txt
    └── shapes.txt
```

# To run excecute the command and follow instruction options
```bash
python gtfs_manager.py
```

## The GTFS Manager makes use of the GTFS Processor. You should not run the processor itself.