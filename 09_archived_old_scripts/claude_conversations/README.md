# Claude Conversation History (Past 7 Days)

This directory contains all your Claude conversations from the past 7 days, organized by date and time.

## File Naming Convention
`YYYY-MM-DD_HHMM_[conversation-id].jsonl`

## Recent Conversations

### September 15, 2025
- `2025-09-15_1347_bc220889-b661-4518-afac-3e589e7237f0.jsonl` (135KB) - Most recent conversation
- `2025-09-15_1337_a20d6cb0-dfe8-48dd-a8bc-60bb20ce6d83.jsonl` (392KB) - Earlier today

### September 12, 2025
- `2025-09-12_2246_0195d037-e8c0-492f-856e-6b12a34aa03f.jsonl` (1.4MB) - Large conversation
- `2025-09-12_2238_ba61f818-7887-45cf-99b2-6804dd9ece71.jsonl` (1.5MB) - React frontend project
- `2025-09-12_1930_deda9465-3783-44bd-9cbd-40ee82c3c178.jsonl` (3KB) - Short conversation
- `2025-09-12_1927_0f2a1522-652d-46e4-88c3-b21434c1fc67.jsonl` (11MB) - Very large conversation
- `2025-09-12_1536_67c7a025-f353-467b-9e69-452530772dff.jsonl` (25KB)

### September 11, 2025
- `2025-09-11_1915_e6e6e0e3-c790-4fb3-8ee6-f5889dba14ff.jsonl` (3.8MB) - PharmMate project

### September 8, 2025
- Multiple conversations throughout the day (13:53 - 18:36)
- Mix of PriceComparisonApp and PharmMate projects

## How to Browse

1. **View a conversation**: Open any `.jsonl` file in a text editor
2. **Search across all conversations**: Use grep from this directory
   ```bash
   grep -r "your search term" *.jsonl
   ```
3. **View conversation summary**: Each line is a JSON message with role (user/assistant) and content

## File Formats

Each `.jsonl` file contains one JSON object per line representing messages in the conversation:
```json
{"role": "user", "content": "your message"}
{"role": "assistant", "content": "claude's response"}
```