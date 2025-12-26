# Location: {{Master_Name}}

<!-- SYSTEM METADATA -->
| System | Master ID | Region | Status |
| :--- | :--- | :--- | :--- |
| **NocoDB** | [Auto-Generated] | Moab Area | Active |

---

## 1. The Core Experience (Shared)
*Content here applies to ALL variations (Standard, Private, Half Day, Full Day).*

**Short Description:**
> [Write the 1 sentence hook here]

**Long Description:**
> [Paste the main website description here. It describes the geology, the views, and the vibe.]

**Highlights:**
*   [Highlight 1]
*   [Highlight 2]

**Images:**
![Feature Image]()

---

## 2. Variations (Arctic Products)
*Populate this table directly from `Arctic_Variants` using the Arctic Reservations field names so integrations stay aligned.*

| `id` (Arctic ID) | `businessgroupid` | `name` | `duration` | `starttime` | `keywords` |
| :--- | :--- | :--- | :--- | :--- | :--- |
| {{variant.id}} | {{variant.businessgroupid}} | {{variant.name}} | {{variant.duration}} | {{variant.starttime}} | {{variant.keywords}} |
| {{...repeat_per_variant...}} |  |  |  |  |  |

---

## 3. Operational Logic (By Business Group)
*Specifics based on the variation type.*

### Half Day Logistics (Groups 5, 10, 21, 22)
*   **Distance:** ~10-15 miles
*   **Duration:** 4 Hours
*   **Meals:** Snacks only
*   **Start Times:** 8:00 AM or 1:00 PM

### Full Day Logistics (Groups 6, 11, 24, 25)
*   **Distance:** ~20-25 miles
*   **Duration:** 7 Hours
*   **Meals:** Lunch included (Deli Sandwiches)
*   **Start Times:** 9:00 AM

---

## 4. Specific Itinerary Data
*If Half Day and Full Day use different routes, define them here.*

| Route Name | Used By | Mileage | Elevation | Campsite/POI |
| :--- | :--- | :--- | :--- | :--- |
| **Intrepid Loop** | Half Day | 12 | 500' | Big Overlook |
| **Twisted Tree** | Full Day | 24 | 1200' | Thelma & Louise Pt |
