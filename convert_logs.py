"""
MTGO Game Log Converter

Converts MTGO game logs to a SQLite database for analysis.
- Tracks processed files to avoid duplicates
- Retains data even if original log files are deleted
- Can be run repeatedly to add new logs

Usage:
    python convert_logs.py <path_to_logs_folder> [database.db]

Example:
    python convert_logs.py "C:\\Users\\paul\\AppData\\Local\\Apps\\2.0\\Data" mtgo_data.db
"""

import sqlite3
import os
import io
import time
import datetime
import modo

# Default paths
DEFAULT_DB = 'mtgo_data.db'
DEFAULT_LOG_FOLDER = r'C:\Users\paul\AppData\Local\Apps\2.0\Data'


def create_database(db_path):
    """Create the SQLite database with all required tables."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Track which files have been processed (prevents duplicates)
    cursor.execute('''CREATE TABLE IF NOT EXISTS Processed_Files (
        Filename TEXT PRIMARY KEY,
        Match_ID TEXT,
        Processed_Date TEXT
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Matches (
        Match_ID TEXT,
        Draft_ID TEXT,
        P1 TEXT,
        P1_Arch TEXT,
        P1_Subarch TEXT,
        P2 TEXT,
        P2_Arch TEXT,
        P2_Subarch TEXT,
        P1_Roll INTEGER,
        P2_Roll INTEGER,
        Roll_Winner TEXT,
        P1_Wins INTEGER,
        P2_Wins INTEGER,
        Match_Winner TEXT,
        Format TEXT,
        Limited_Format TEXT,
        Match_Type TEXT,
        Date TEXT,
        PRIMARY KEY (Match_ID, P1)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Games (
        Match_ID TEXT,
        P1 TEXT,
        P2 TEXT,
        Game_Num INTEGER,
        PD_Selector TEXT,
        PD_Choice TEXT,
        On_Play TEXT,
        On_Draw TEXT,
        P1_Mulls INTEGER,
        P2_Mulls INTEGER,
        Turns INTEGER,
        Game_Winner TEXT,
        PRIMARY KEY (Match_ID, Game_Num, P1)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Plays (
        Match_ID TEXT,
        Game_Num INTEGER,
        Play_Num INTEGER,
        Turn_Num INTEGER,
        Casting_Player TEXT,
        Action TEXT,
        Primary_Card TEXT,
        Target1 TEXT,
        Target2 TEXT,
        Target3 TEXT,
        Opp_Target INTEGER,
        Self_Target INTEGER,
        Cards_Drawn INTEGER,
        Attackers INTEGER,
        Active_Player TEXT,
        Nonactive_Player TEXT
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS GameActions (
        Match_ID TEXT,
        Game_Num INTEGER,
        Game_Actions TEXT,
        PRIMARY KEY (Match_ID, Game_Num)
    )''')

    conn.commit()
    return conn


def get_processed_files(conn):
    """Get set of already processed filenames."""
    cursor = conn.cursor()
    cursor.execute('SELECT Filename FROM Processed_Files')
    return {row[0] for row in cursor.fetchall()}


def find_log_files(folder_path):
    """Recursively find all Match_GameLog_*.dat files."""
    log_files = []
    for root, dirs, files in os.walk(folder_path):
        for f in files:
            if 'Match_GameLog_' in f and f.endswith('.dat'):
                log_files.append(os.path.join(root, f))
    return log_files


def convert_logs(log_folder=None, db_path=None):
    """
    Convert all new game logs in folder to SQLite database.

    - Skips files that have already been processed
    - Retains all existing data in the database
    - Can be run multiple times safely
    """
    log_folder = log_folder or DEFAULT_LOG_FOLDER
    db_path = db_path or DEFAULT_DB

    # Check if database exists (for messaging)
    db_exists = os.path.exists(db_path)

    if db_exists:
        print(f"Updating existing database: {db_path}")
    else:
        print(f"Creating new database: {db_path}")

    conn = create_database(db_path)
    cursor = conn.cursor()

    # Get already processed files
    processed_files = get_processed_files(conn)
    print(f"Already processed: {len(processed_files)} files")

    print(f"Scanning for log files in: {log_folder}")
    log_files = find_log_files(log_folder)
    print(f"Found {len(log_files)} game log files")

    # Filter out already processed files
    new_files = [f for f in log_files if os.path.basename(f) not in processed_files]
    print(f"New files to process: {len(new_files)}\n")

    if not new_files:
        print("No new game logs to process.")
        print_summary(conn)
        conn.close()
        return db_path

    match_count = 0
    error_count = 0
    skipped_count = 0
    all_matches = []
    all_games = []
    all_plays = []
    all_actions = {}

    for filepath in new_files:
        filename = os.path.basename(filepath)
        try:
            with io.open(filepath, 'r', encoding='latin1') as f:
                content = f.read()

            fname = filename.split('Match_GameLog_')[1].split('.dat')[0]
            mtime = time.ctime(os.path.getmtime(filepath))

            parsed_data = modo.get_all_data(content, mtime, fname)

            if isinstance(parsed_data, str):
                print(f"  Skipped: {filename} - {parsed_data}")
                skipped_count += 1
                continue

            match_id = parsed_data[0][0]

            all_matches.append(parsed_data[0])
            all_games.extend(parsed_data[1])
            all_plays.extend(parsed_data[2])
            all_actions.update(parsed_data[3])

            # Record that we processed this file
            cursor.execute(
                'INSERT OR IGNORE INTO Processed_Files VALUES (?, ?, ?)',
                [filename, match_id, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            )

            match_count += 1
            if match_count % 10 == 0:
                print(f"  Processed {match_count} matches...")

        except Exception as e:
            print(f"  Error parsing {filename}: {e}")
            error_count += 1

    print(f"\nParsed {match_count} new matches ({error_count} errors, {skipped_count} skipped)")

    if match_count > 0:
        # Create inverted view (both player perspectives)
        print("Creating player perspectives...")
        all_data = [all_matches, all_games, all_plays, all_actions]
        inverted_data = modo.invert_join(all_data)

        # Insert data (OR IGNORE prevents duplicates)
        print("Inserting matches...")
        cursor.executemany(
            'INSERT OR IGNORE INTO Matches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            inverted_data[0]
        )

        print("Inserting games...")
        cursor.executemany(
            'INSERT OR IGNORE INTO Games VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            inverted_data[1]
        )

        print("Inserting plays...")
        cursor.executemany(
            'INSERT OR IGNORE INTO Plays VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            inverted_data[2]
        )

        print("Inserting game actions...")
        for key, value in inverted_data[3].items():
            cursor.execute(
                'INSERT OR IGNORE INTO GameActions VALUES (?,?,?)',
                [key[:-2], int(key[-1]), '\n'.join(value[-15:])]
            )

        conn.commit()
        print(f"\nAdded {match_count} new matches to database.")

    print_summary(conn)
    conn.close()
    return db_path


def print_summary(conn):
    """Print database summary statistics."""
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM Processed_Files')
    files = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(DISTINCT Match_ID) FROM Matches')
    matches = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM Games')
    games = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM Plays')
    plays = cursor.fetchone()[0]

    print(f"\n{'='*40}")
    print(f"DATABASE SUMMARY")
    print(f"{'='*40}")
    print(f"Log files processed: {files}")
    print(f"Unique matches:      {matches}")
    print(f"Game records:        {games}")
    print(f"Play records:        {plays}")
    print(f"{'='*40}")
    print(f"Database location: {os.path.abspath(conn.execute('PRAGMA database_list').fetchone()[2])}")


def show_stats(db_path=None):
    """Show basic statistics from the database."""
    db_path = db_path or DEFAULT_DB

    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print(f"\n{'='*50}")
    print("MTGO STATISTICS")
    print(f"{'='*50}\n")

    # Get all unique P1 players and their match counts
    cursor.execute('''
        SELECT P1, COUNT(*) as matches
        FROM Matches
        GROUP BY P1
        ORDER BY matches DESC
        LIMIT 1
    ''')
    result = cursor.fetchone()
    if result:
        hero = result[0]
        print(f"Main player (most matches): {hero}\n")

        # Win rate for main player
        cursor.execute('''
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN Match_Winner = 'P1' THEN 1 ELSE 0 END) as wins
            FROM Matches
            WHERE P1 = ?
        ''', [hero])
        total, wins = cursor.fetchone()
        wins = wins or 0
        wr = (wins / total * 100) if total > 0 else 0
        print(f"Match Record: {wins}-{total-wins} ({wr:.1f}% win rate)\n")

        # Recent matches
        print("Recent Matches:")
        cursor.execute('''
            SELECT Date, P2,
                   CASE WHEN Match_Winner = 'P1' THEN 'Won' ELSE 'Lost' END as result,
                   P1_Wins || '-' || P2_Wins as score
            FROM Matches
            WHERE P1 = ?
            ORDER BY Date DESC
            LIMIT 10
        ''', [hero])
        for row in cursor.fetchall():
            print(f"  {row[0]}: vs {row[1]} - {row[2]} ({row[3]})")

    conn.close()


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        # No arguments - use defaults
        print("No arguments provided. Using defaults:")
        print(f"  Log folder: {DEFAULT_LOG_FOLDER}")
        print(f"  Database:   {DEFAULT_DB}\n")
        convert_logs()
    elif sys.argv[1] == '--stats':
        # Show statistics
        db = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DB
        show_stats(db)
    elif sys.argv[1] == '--help':
        print(__doc__)
        print("\nCommands:")
        print("  python convert_logs.py                     - Update database with new logs (uses defaults)")
        print("  python convert_logs.py <folder>            - Process logs from specific folder")
        print("  python convert_logs.py <folder> <db.db>    - Process logs to specific database")
        print("  python convert_logs.py --stats             - Show statistics from database")
        print("  python convert_logs.py --stats <db.db>     - Show statistics from specific database")
    else:
        log_folder = sys.argv[1]
        db_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DB

        if not os.path.exists(log_folder):
            print(f"Error: Folder not found: {log_folder}")
            sys.exit(1)

        convert_logs(log_folder, db_path)
