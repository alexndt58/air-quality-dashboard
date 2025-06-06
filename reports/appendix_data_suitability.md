﻿## Appendix A  Data-suitability Note

**Coverage**  01 Jan 2025 00:00  18 May 2025 09:00 BST, 96 monitoring sites  
**Completeness**  NO 0.6 % gaps, PM. 1.2 % (calibration outages)  
**Key variables**  `no2`, `pm25`, `site_name`, `latitude`, `longitude`,  
`temp`, `wind_speed` (from Met Office)  

**Fitness for purpose**  
Hourly resolution matches commuter decision-making; average site spacing  
(< 3 km in Greater London) is good enough for route suggestions.

**Risks & mitigations**

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| 30 MB data per day grows fast | Medium | Stream into DuckDB and discard > 365 days |
| Site outages > 2 h | Low | Forward-fill 2 h, flag longer gaps in UI |
| Licensing issues | Low | Data under OGL v3  include attribution |
| Privacy / PII | None | Environmental data only |

