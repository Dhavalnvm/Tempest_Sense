cyclone_tracker/
│
├── lib/
│   ├── main.dart                    # App entry point
│   │
│   ├── models/                      # Data models
│   │   ├── cyclone.dart             # Cyclone data model
│   │   └── api_response.dart        # API response models
│   │
│   ├── services/                    # Business logic
│   │   ├── api_service.dart         # API client
│   │   ├── location_service.dart    # GPS/location
│   │   └── cache_service.dart       # Local caching
│   │
│   ├── screens/                     # UI screens
│   │   ├── home_screen.dart         # Main map view
│   │   ├── cyclone_detail_screen.dart
│   │   ├── list_screen.dart         # List view
│   │   └── settings_screen.dart
│   │
│   ├── widgets/                     # Reusable components
│   │   ├── cyclone_marker.dart      # Map marker
│   │   ├── cyclone_card.dart        # List item card
│   │   ├── intensity_badge.dart     # Color-coded badge
│   │   └── forecast_chart.dart      # Forecast visualization
│   │
│   ├── providers/                   # State management
│   │   ├── cyclone_provider.dart    # Cyclone data state
│   │   └── settings_provider.dart   # App settings
│   │
│   ├── utils/                       # Utilities
│   │   ├── constants.dart           # App constants
│   │   ├── colors.dart              # Color scheme
│   │   └── formatters.dart          # Data formatters
│   │
│   └── config/
│       └── app_config.dart          # Configuration
│
├── assets/                          # Static assets
│   ├── icons/
│   └── images/
│
├── test/                            # Unit tests
│   ├── models_test.dart
│   ├── services_test.dart
│   └── widgets_test.dart
│
├── pubspec.yaml                     # Dependencies
├── android/                         # Android config
├── ios/                            # iOS config
└── web/                            # Web config