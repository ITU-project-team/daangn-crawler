"""Generate 2,000-dbId work chunks and save them to chunks.txt."""

START  = 733_968_953
END    = 727_174_000
CHUNK  = 2_000
PEOPLE = 5

chunks = []
cur = START
idx = 1
while cur > END:
    chunk_end = max(cur - CHUNK + 1, END)
    chunks.append((idx, cur, chunk_end))
    cur = chunk_end - 1
    idx += 1

total = len(chunks)
per_person = total // PEOPLE
remainder  = total % PEOPLE

# Split chunks across five workers as evenly as possible.
assignments = []
start_i = 0
for person in range(1, PEOPLE + 1):
    count = per_person + (1 if person <= remainder else 0)
    end_i = start_i + count - 1
    assignments.append((person, start_i, end_i, chunks[start_i], chunks[end_i]))
    start_i = end_i + 1

with open("chunks.txt", "w", encoding="utf-8") as f:
    f.write(f"total dbId range: {START:,} ~ {END:,}  |  chunk size: {CHUNK:,}  |  total chunks: {total:,}\n")
    f.write("=" * 70 + "\n\n")

    f.write("[5worker distribution summary]\n")
    for person, si, ei, first, last in assignments:
        f.write(
            f"  worker {person}: chunk {first[0]:>4} ~ {last[0]:>4}  "
            f"({last[2]:,} ~ {first[1]:,})  "
            f"({ei - si + 1} chunks / {first[1] - last[2] + 1:,} dbIds)\n"
        )
    f.write("\n" + "=" * 70 + "\n\n")

    f.write(f"{'chunk':>6}  {'start_dbid':>13}  {'end_dbid':>13}  {'worker':>6}\n")
    f.write("-" * 50 + "\n")
    for person, si, ei, first, last in assignments:
        for i in range(si, ei + 1):
            cidx, cs, ce = chunks[i]
            f.write(f"{cidx:>6}  {cs:>13,}  {ce:>13,}  {person:>6}\n")

print(f"chunks.txt save complete (total {total} chunks, about {per_person} per worker)")
print()
print("[5worker distribution summary]")
for person, si, ei, first, last in assignments:
    print(
        f"  worker {person}: chunk {first[0]:>4} ~ {last[0]:>4}  "
        f"(start={first[1]:,} / end={last[2]:,})  "
        f"{ei - si + 1} chunks"
    )
