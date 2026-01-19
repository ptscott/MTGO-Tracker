"""
Premodern MTG Tournament Scraper

Scrapes tournament results and decklists from MTGTop8 for the Premodern format.
Stores data in SQLite for analysis.

Usage:
    python scrape_premodern.py                  # Scrape recent events
    python scrape_premodern.py --events 50      # Scrape last 50 events
    python scrape_premodern.py --stats          # Show metagame statistics
    python scrape_premodern.py --decks Goblins  # Show decks of archetype
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import time
import os
import sys
from datetime import datetime
from urllib.parse import urljoin
import argparse

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_URL = "https://www.mtgtop8.com/"
DB_PATH = "premodern_data.db"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Rate limiting
REQUEST_DELAY = 1.0  # seconds between requests


def create_database(db_path):
    """Create the SQLite database with required tables."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS Events (
        event_id INTEGER PRIMARY KEY,
        name TEXT,
        date TEXT,
        location TEXT,
        num_players INTEGER,
        scraped_date TEXT
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Decks (
        deck_id INTEGER PRIMARY KEY,
        event_id INTEGER,
        player TEXT,
        archetype TEXT,
        placement TEXT,
        placement_num INTEGER,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Cards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deck_id INTEGER,
        card_name TEXT,
        quantity INTEGER,
        is_sideboard INTEGER,
        FOREIGN KEY (deck_id) REFERENCES Decks(deck_id)
    )''')

    cursor.execute('''CREATE INDEX IF NOT EXISTS idx_cards_deck ON Cards(deck_id)''')
    cursor.execute('''CREATE INDEX IF NOT EXISTS idx_cards_name ON Cards(card_name)''')
    cursor.execute('''CREATE INDEX IF NOT EXISTS idx_decks_archetype ON Decks(archetype)''')

    conn.commit()
    return conn


def get_soup(url):
    """Fetch URL and return BeautifulSoup object."""
    time.sleep(REQUEST_DELAY)
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return BeautifulSoup(response.text, 'html.parser')


def parse_placement(placement_text):
    """Convert placement text to numeric value for sorting."""
    placement_text = placement_text.strip()
    if placement_text == "1":
        return 1
    elif placement_text == "2":
        return 2
    elif placement_text in ["3-4", "3"]:
        return 3
    elif placement_text in ["5-8", "5"]:
        return 5
    elif placement_text in ["9-16", "9"]:
        return 9
    else:
        # Try to extract first number
        match = re.search(r'(\d+)', placement_text)
        return int(match.group(1)) if match else 99


def scrape_event_list(num_events=20):
    """Scrape list of recent Premodern events."""
    events = []
    page = 1

    while len(events) < num_events:
        url = f"{BASE_URL}format?f=PREM&cp={page}"
        print(f"Fetching event list page {page}...")

        try:
            soup = get_soup(url)
        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            break

        # Find event links
        event_links = soup.find_all('a', href=re.compile(r'event\?e=\d+'))

        if not event_links:
            break

        for link in event_links:
            href = link.get('href', '')
            match = re.search(r'e=(\d+)', href)
            if match:
                event_id = int(match.group(1))
                event_name = link.get_text(strip=True)

                # Skip if already have this event
                if any(e['id'] == event_id for e in events):
                    continue

                # Try to get date and location from surrounding elements
                parent = link.find_parent('tr') or link.find_parent('div')
                date_text = ""
                location = ""

                if parent:
                    text = parent.get_text()
                    # Look for date pattern MM/DD/YY
                    date_match = re.search(r'(\d{2}/\d{2}/\d{2})', text)
                    if date_match:
                        date_text = date_match.group(1)

                events.append({
                    'id': event_id,
                    'name': event_name,
                    'date': date_text,
                    'location': location
                })

                if len(events) >= num_events:
                    break

        page += 1
        if page > 10:  # Safety limit
            break

    return events[:num_events]


def scrape_event(event_id):
    """Scrape a single event's decklists."""
    url = f"{BASE_URL}event?e={event_id}&f=PREM"
    print(f"  Fetching event {event_id}...")

    try:
        soup = get_soup(url)
    except Exception as e:
        print(f"  Error fetching event: {e}")
        return None, []

    # Get event details - look for event name in title or header
    # The w_title often contains extra info, so try to extract just the event name
    event_name = f"Event {event_id}"
    title_elem = soup.find('div', class_='w_title')
    if title_elem:
        # Event name is usually before the "#1" winner indicator
        title_text = title_elem.get_text(strip=True)
        # Split on #1, #2 etc to get just the event name
        name_match = re.match(r'^(.+?)(?:#\d|$)', title_text)
        if name_match:
            event_name = name_match.group(1).strip()

    # Find all deck entries
    decks = []
    seen_deck_ids = set()

    # MTGTop8 structure: S14 divs alternate between placement numbers and deck names
    # Placement: S14 div with just a number (no deck link)
    # Deck: S14 div with a deck link containing the archetype name
    s14_divs = soup.find_all('div', class_='S14')

    for i, div in enumerate(s14_divs):
        deck_link = div.find('a', href=re.compile(r'd=\d+'))
        if not deck_link:
            continue

        href = deck_link.get('href', '')
        match = re.search(r'd=(\d+)', href)
        if not match:
            continue

        deck_id = int(match.group(1))
        if deck_id in seen_deck_ids:
            continue
        seen_deck_ids.add(deck_id)

        # Archetype is the text of the deck link in S14 div
        archetype = deck_link.get_text(strip=True)

        # Find player name - in a previous link with search?player
        player = "Unknown"
        player_link = div.find_previous('a', href=re.compile(r'search\?player'))
        if player_link:
            player = player_link.get_text(strip=True)

        # Find placement - look at previous S14 siblings for a number pattern
        placement = ""
        prev_s14 = div.find_previous('div', class_='S14')
        if prev_s14:
            text = prev_s14.get_text(strip=True)
            if re.match(r'^\d+(-\d+)?$', text):
                placement = text

        decks.append({
            'deck_id': deck_id,
            'archetype': archetype,
            'player': player,
            'placement': placement,
            'placement_num': parse_placement(placement)
        })

    return event_name, decks


def scrape_decklist(event_id, deck_id):
    """Scrape individual decklist using MTGO export format."""
    # Use the MTGO export URL which returns plain text
    url = f"{BASE_URL}mtgo?d={deck_id}"

    try:
        time.sleep(REQUEST_DELAY)
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        text = response.text
    except Exception as e:
        print(f"    Error fetching deck {deck_id}: {e}")
        return [], []

    maindeck = []
    sideboard = []

    lines = text.split('\n')
    in_sideboard = False

    for line in lines:
        line = line.strip()

        # Skip empty lines
        if not line:
            continue

        # Check for sideboard marker
        if line.upper().startswith('SIDEBOARD') or line.upper() == 'SB:':
            in_sideboard = True
            continue

        # Match "N CardName" pattern
        match = re.match(r'^(\d+)\s+(.+)$', line)
        if match:
            qty = int(match.group(1))
            card = match.group(2).strip()

            # Filter out section headers and non-cards
            if len(card) < 2 or len(card) > 60:
                continue
            # Skip category headers like "4 CREATURES" or "19 LANDS"
            if card.isupper() and any(x in card for x in ['LAND', 'CREATURE', 'INSTANT', 'SORCERY', 'ARTIFACT', 'ENCHANT', 'PLANESWALKER']):
                continue

            if in_sideboard:
                sideboard.append((card, qty))
            else:
                maindeck.append((card, qty))

    return maindeck, sideboard


def scrape_events(conn, num_events=20):
    """Main scraping function."""
    cursor = conn.cursor()

    # Get already scraped events
    cursor.execute('SELECT event_id FROM Events')
    existing_events = {row[0] for row in cursor.fetchall()}

    print(f"Already have {len(existing_events)} events in database")
    print(f"Fetching up to {num_events} recent events...")

    events = scrape_event_list(num_events)
    print(f"Found {len(events)} events")

    new_events = 0
    new_decks = 0

    for event in events:
        event_id = event['id']

        if event_id in existing_events:
            print(f"Skipping event {event_id} (already scraped)")
            continue

        event_name, decks = scrape_event(event_id)

        if not decks:
            print(f"  No decks found for event {event_id}")
            continue

        # Insert event
        cursor.execute('''
            INSERT OR REPLACE INTO Events (event_id, name, date, location, num_players, scraped_date)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (event_id, event_name, event['date'], event['location'],
              len(decks), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        new_events += 1

        # Scrape each decklist
        for deck in decks:
            deck_id = deck['deck_id']
            print(f"    Scraping deck {deck_id} ({deck['archetype']})...")

            maindeck, sideboard = scrape_decklist(event_id, deck_id)

            if not maindeck:
                print(f"    No cards found for deck {deck_id}")
                continue

            # Insert deck
            cursor.execute('''
                INSERT OR REPLACE INTO Decks (deck_id, event_id, player, archetype, placement, placement_num)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (deck_id, event_id, deck['player'], deck['archetype'],
                  deck['placement'], deck['placement_num']))

            # Delete old cards for this deck
            cursor.execute('DELETE FROM Cards WHERE deck_id = ?', (deck_id,))

            # Insert maindeck cards
            for card, qty in maindeck:
                cursor.execute('''
                    INSERT INTO Cards (deck_id, card_name, quantity, is_sideboard)
                    VALUES (?, ?, ?, 0)
                ''', (deck_id, card, qty))

            # Insert sideboard cards
            for card, qty in sideboard:
                cursor.execute('''
                    INSERT INTO Cards (deck_id, card_name, quantity, is_sideboard)
                    VALUES (?, ?, ?, 1)
                ''', (deck_id, card, qty))

            new_decks += 1

        conn.commit()

    print(f"\nScraped {new_events} new events with {new_decks} decklists")
    return new_events, new_decks


def show_metagame_stats(conn):
    """Display metagame statistics."""
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("PREMODERN METAGAME STATISTICS")
    print("=" * 60)

    # Overall stats
    cursor.execute('SELECT COUNT(*) FROM Events')
    num_events = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM Decks')
    num_decks = cursor.fetchone()[0]

    print(f"\nTotal Events: {num_events}")
    print(f"Total Decklists: {num_decks}")

    # Archetype popularity
    print("\n" + "-" * 60)
    print("ARCHETYPE POPULARITY (Top 20)")
    print("-" * 60)

    cursor.execute('''
        SELECT archetype, COUNT(*) as count,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Decks), 1) as pct
        FROM Decks
        GROUP BY archetype
        ORDER BY count DESC
        LIMIT 20
    ''')

    print(f"{'Archetype':<30} {'Count':>8} {'%':>8}")
    print("-" * 48)
    for row in cursor.fetchall():
        print(f"{row[0]:<30} {row[1]:>8} {row[2]:>7.1f}%")

    # Top 8 performance by archetype
    print("\n" + "-" * 60)
    print("TOP 8 PERFORMANCE BY ARCHETYPE")
    print("-" * 60)

    cursor.execute('''
        SELECT archetype,
               COUNT(*) as total,
               SUM(CASE WHEN placement_num <= 8 THEN 1 ELSE 0 END) as top8,
               SUM(CASE WHEN placement_num = 1 THEN 1 ELSE 0 END) as wins
        FROM Decks
        WHERE placement_num > 0
        GROUP BY archetype
        HAVING total >= 5
        ORDER BY (wins * 1.0 / total) DESC
        LIMIT 15
    ''')

    print(f"{'Archetype':<25} {'Total':>7} {'Top 8':>7} {'Wins':>6} {'Win%':>7}")
    print("-" * 55)
    for row in cursor.fetchall():
        win_pct = (row[3] / row[1] * 100) if row[1] > 0 else 0
        print(f"{row[0]:<25} {row[1]:>7} {row[2]:>7} {row[3]:>6} {win_pct:>6.1f}%")

    # Most played cards
    print("\n" + "-" * 60)
    print("MOST PLAYED CARDS (Maindeck)")
    print("-" * 60)

    cursor.execute('''
        SELECT card_name,
               COUNT(DISTINCT deck_id) as decks,
               SUM(quantity) as total_copies,
               ROUND(COUNT(DISTINCT deck_id) * 100.0 / (SELECT COUNT(*) FROM Decks), 1) as pct
        FROM Cards
        WHERE is_sideboard = 0
        GROUP BY card_name
        ORDER BY decks DESC
        LIMIT 25
    ''')

    print(f"{'Card':<35} {'Decks':>7} {'Copies':>8} {'%':>7}")
    print("-" * 60)
    for row in cursor.fetchall():
        print(f"{row[0]:<35} {row[1]:>7} {row[2]:>8} {row[3]:>6.1f}%")

    # Most played sideboard cards
    print("\n" + "-" * 60)
    print("MOST PLAYED SIDEBOARD CARDS")
    print("-" * 60)

    cursor.execute('''
        SELECT card_name,
               COUNT(DISTINCT deck_id) as decks,
               SUM(quantity) as total_copies
        FROM Cards
        WHERE is_sideboard = 1
        GROUP BY card_name
        ORDER BY decks DESC
        LIMIT 15
    ''')

    print(f"{'Card':<35} {'Decks':>7} {'Copies':>8}")
    print("-" * 53)
    for row in cursor.fetchall():
        print(f"{row[0]:<35} {row[1]:>7} {row[2]:>8}")


def show_archetype_decks(conn, archetype):
    """Show recent decks of a specific archetype."""
    cursor = conn.cursor()

    print(f"\n{'=' * 60}")
    print(f"RECENT {archetype.upper()} DECKLISTS")
    print("=" * 60)

    cursor.execute('''
        SELECT d.deck_id, d.player, d.placement, e.name, e.date
        FROM Decks d
        JOIN Events e ON d.event_id = e.event_id
        WHERE d.archetype LIKE ?
        ORDER BY e.date DESC, d.placement_num
        LIMIT 10
    ''', (f'%{archetype}%',))

    decks = cursor.fetchall()

    if not decks:
        print(f"No decks found matching '{archetype}'")
        return

    for deck_id, player, placement, event, date in decks:
        print(f"\n{'-' * 50}")
        print(f"Player: {player}")
        print(f"Placement: {placement}")
        print(f"Event: {event} ({date})")
        print(f"{'-' * 50}")

        # Get maindeck
        cursor.execute('''
            SELECT card_name, quantity FROM Cards
            WHERE deck_id = ? AND is_sideboard = 0
            ORDER BY quantity DESC, card_name
        ''', (deck_id,))

        print("\nMaindeck:")
        for card, qty in cursor.fetchall():
            print(f"  {qty} {card}")

        # Get sideboard
        cursor.execute('''
            SELECT card_name, quantity FROM Cards
            WHERE deck_id = ? AND is_sideboard = 1
            ORDER BY quantity DESC, card_name
        ''', (deck_id,))

        sb_cards = cursor.fetchall()
        if sb_cards:
            print("\nSideboard:")
            for card, qty in sb_cards:
                print(f"  {qty} {card}")


def main():
    parser = argparse.ArgumentParser(description='Scrape Premodern MTG tournament data')
    parser.add_argument('--events', type=int, default=20, help='Number of events to scrape')
    parser.add_argument('--stats', action='store_true', help='Show metagame statistics')
    parser.add_argument('--decks', type=str, help='Show decks of specific archetype')
    parser.add_argument('--db', type=str, default=DB_PATH, help='Database path')

    args = parser.parse_args()

    conn = create_database(args.db)

    if args.stats:
        show_metagame_stats(conn)
    elif args.decks:
        show_archetype_decks(conn, args.decks)
    else:
        scrape_events(conn, args.events)
        show_metagame_stats(conn)

    conn.close()


if __name__ == '__main__':
    main()
