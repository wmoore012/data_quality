# Data Quality

[![CI](https://github.com/wmoore012/data_quality/actions/workflows/ci.yml/badge.svg)](https://github.com/wmoore012/data_quality/actions/workflows/ci.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/wmoore012/data_quality/blob/main/LICENSE)

> **Built for [Perday CatalogLAB](https://perdaycatalog.com)** - a live demo of a data story platform for music producers and songwriters.
>
> [![CatalogLAB Data Story](docs/data_story.png)](https://perdaycatalog.com)

Comprehensive data quality monitoring and validation for music analytics.

**Repo:** https://github.com/wmoore012/data_quality
**What it does:** Validates catalog data against music industry standards, flags anomalies, and generates quality reports that CatalogLAB uses to decide if data is trustworthy.

## Why I Built It

Streaming platforms, distributors, and labels all report data differently. Views might be missing. ISRCs might be malformed. Release dates might be in the future. If your analytics are built on garbage, your insights are garbage.

I built `data_quality` to create a "4-Way Gate" system for CatalogLAB:
1. **Volume Gate**: Do we have enough songs to tell a meaningful story?
2. **VIP Gate**: Are key artists present in the data?
3. **Engagement Gate**: Do the numbers make sense (views vs comments)?
4. **Source Gate**: Is this real data or demo/mock data?

If any gate fails, CatalogLAB withholds the data story rather than show something misleading.

## Key Features

- **Schema validators** for ISRCs, UPCs, release dates, and streaming metrics
- **Anomaly detection** for impossible values (negative views, future releases)
- **Completeness scoring** to measure data coverage
- **Configurable thresholds** via environment variables or code
- **Structured reports** in JSON for downstream processing

## Installation

```bash
pip install data-quality
```

Or clone locally:

```bash
git clone https://github.com/wmoore012/data_quality.git
cd data_quality
pip install -e .
```

## Quick Start

```python
from data_quality import validate_catalog, GateConfig

config = GateConfig(
    min_songs=100,
    required_artists=["Rapper Big Pooh", "Lute"],
    max_views_without_comments=1000
)

result = validate_catalog(my_catalog_data, config)

if result.passed:
    print("Data is trustworthy!")
else:
    print(f"Gates failed: {result.errors}")
```

## Performance

| Metric | Value |
|--------|-------|
| Validation speed | 50K rows/sec |
| Memory footprint | < 50MB for 1M rows |
| Report generation | < 100ms |

See [BENCHMARKS.md](BENCHMARKS.md) for detailed results.

## Documentation

- [API Documentation](docs/)
- [Examples](examples/)
- [Contributing Guide](CONTRIBUTING.md)
- [Security Policy](SECURITY.md)

## Professional Context

Built by **Wilton Moore** for Perday Labs. This module demonstrates:

- Data validation patterns for entertainment/financial data
- Configurable quality gates for production pipelines
- The difference between "data exists" and "data is trustworthy"

## Contact

Want to talk about data quality in music analytics?
- LinkedIn: https://www.linkedin.com/in/wiltonmoore/
- GitHub: https://github.com/wmoore012

## License

MIT License. See [LICENSE](LICENSE) for details.
