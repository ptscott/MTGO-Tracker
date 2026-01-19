"""Show detailed statistics from the MTGO database."""

import sqlite3
import pandas as pd
import os

DB_PATH = 'mtgo_data.db'

def main():
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    # Find main player
    cursor = conn.cursor()
    cursor.execute('''
        SELECT P1, COUNT(*) as cnt FROM Matches
        GROUP BY P1 ORDER BY cnt DESC LIMIT 1
    ''')
    hero = cursor.fetchone()[0]

    print('=' * 60)
    print(f'MTGO DATABASE SUMMARY FOR {hero}')
    print('=' * 60)

    # Overall record
    df = pd.read_sql(f'''
        SELECT
            COUNT(*) as Matches,
            SUM(CASE WHEN Match_Winner = 'P1' THEN 1 ELSE 0 END) as Wins,
            SUM(CASE WHEN Match_Winner = 'P2' THEN 1 ELSE 0 END) as Losses
        FROM Matches WHERE P1 = '{hero}'
    ''', conn)
    wins, losses = int(df['Wins'].iloc[0] or 0), int(df['Losses'].iloc[0] or 0)
    wr = wins/(wins+losses)*100 if (wins+losses) > 0 else 0
    print(f'\nOVERALL RECORD: {wins}-{losses} ({wr:.1f}% win rate)')
    print(f'Total Matches: {df["Matches"].iloc[0]}')

    # Game record
    df = pd.read_sql(f'''
        SELECT
            SUM(CASE WHEN Game_Winner = 'P1' THEN 1 ELSE 0 END) as Wins,
            SUM(CASE WHEN Game_Winner = 'P2' THEN 1 ELSE 0 END) as Losses
        FROM Games WHERE P1 = '{hero}'
    ''', conn)
    gw, gl = int(df['Wins'].iloc[0] or 0), int(df['Losses'].iloc[0] or 0)
    gwr = gw/(gw+gl)*100 if (gw+gl) > 0 else 0
    print(f'Game Record: {gw}-{gl} ({gwr:.1f}% win rate)')

    # Date range
    df = pd.read_sql(f'SELECT MIN(Date) as First, MAX(Date) as Last FROM Matches WHERE P1="{hero}"', conn)
    print(f'Date Range: {df["First"].iloc[0]} to {df["Last"].iloc[0]}')

    # Most played cards
    print('\n' + '=' * 60)
    print(f'MOST PLAYED CARDS (by {hero})')
    print('=' * 60)
    df = pd.read_sql(f'''
        SELECT
            Primary_Card as Card,
            COUNT(*) as Times_Cast
        FROM Plays p
        JOIN Games g ON p.Match_ID = g.Match_ID AND p.Game_Num = g.Game_Num
        WHERE g.P1 = '{hero}'
          AND p.Casting_Player = '{hero}'
          AND p.Action = 'Casts'
          AND p.Primary_Card <> 'NA'
          AND p.Primary_Card IS NOT NULL
        GROUP BY Primary_Card
        ORDER BY Times_Cast DESC
        LIMIT 20
    ''', conn)
    print(df.to_string(index=False))

    # Cards cast against you
    print('\n' + '=' * 60)
    print('CARDS CAST AGAINST YOU (by opponents)')
    print('=' * 60)
    df = pd.read_sql(f'''
        SELECT
            Primary_Card as Card,
            COUNT(*) as Times_Cast
        FROM Plays p
        JOIN Games g ON p.Match_ID = g.Match_ID AND p.Game_Num = g.Game_Num
        WHERE g.P1 = '{hero}'
          AND p.Casting_Player <> '{hero}'
          AND p.Action = 'Casts'
          AND p.Primary_Card <> 'NA'
          AND p.Primary_Card IS NOT NULL
        GROUP BY Primary_Card
        ORDER BY Times_Cast DESC
        LIMIT 20
    ''', conn)
    print(df.to_string(index=False))

    # Win rate by card
    print('\n' + '=' * 60)
    print('WIN RATE WHEN YOU CAST SPECIFIC CARDS')
    print('=' * 60)
    df = pd.read_sql(f'''
        SELECT
            p.Primary_Card as Card,
            COUNT(DISTINCT p.Match_ID || '-' || p.Game_Num) as Games_Cast,
            SUM(CASE WHEN g.Game_Winner = 'P1' THEN 1 ELSE 0 END) as Wins
        FROM Plays p
        JOIN Games g ON p.Match_ID = g.Match_ID AND p.Game_Num = g.Game_Num
        WHERE g.P1 = '{hero}'
          AND p.Casting_Player = '{hero}'
          AND p.Action = 'Casts'
          AND p.Primary_Card <> 'NA'
        GROUP BY p.Primary_Card
        HAVING Games_Cast >= 5
        ORDER BY Games_Cast DESC
        LIMIT 15
    ''', conn)
    df['Win%'] = (df['Wins'] / df['Games_Cast'] * 100).round(1)
    print(df.to_string(index=False))

    # Match results breakdown
    print('\n' + '=' * 60)
    print('MATCH RESULTS SUMMARY')
    print('=' * 60)
    df = pd.read_sql(f'''
        SELECT
            P1_Wins || '-' || P2_Wins as Score,
            COUNT(*) as Matches,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Matches WHERE P1='{hero}'), 1) as Pct
        FROM Matches
        WHERE P1 = '{hero}'
        GROUP BY P1_Wins, P2_Wins
        ORDER BY
            CASE WHEN P1_Wins > P2_Wins THEN 0 ELSE 1 END,
            Matches DESC
    ''', conn)
    print(df.to_string(index=False))

    # Record by day
    print('\n' + '=' * 60)
    print('RECORD BY DAY')
    print('=' * 60)
    df = pd.read_sql(f'''
        SELECT
            SUBSTR(Date, 1, 10) as Day,
            COUNT(*) as Matches,
            SUM(CASE WHEN Match_Winner = 'P1' THEN 1 ELSE 0 END) as Wins,
            SUM(CASE WHEN Match_Winner = 'P2' THEN 1 ELSE 0 END) as Losses
        FROM Matches
        WHERE P1 = '{hero}'
        GROUP BY SUBSTR(Date, 1, 10)
        ORDER BY Day DESC
    ''', conn)
    df['Win%'] = (df['Wins'] / df['Matches'] * 100).round(1)
    print(df.to_string(index=False))

    # Mulligan stats
    print('\n' + '=' * 60)
    print('MULLIGAN STATISTICS')
    print('=' * 60)
    df = pd.read_sql(f'''
        SELECT
            P1_Mulls as Mulligans,
            COUNT(*) as Games,
            SUM(CASE WHEN Game_Winner = 'P1' THEN 1 ELSE 0 END) as Wins
        FROM Games
        WHERE P1 = '{hero}'
        GROUP BY P1_Mulls
        ORDER BY P1_Mulls
    ''', conn)
    df['Win%'] = (df['Wins'] / df['Games'] * 100).round(1)
    print(df.to_string(index=False))

    # Game length
    print('\n' + '=' * 60)
    print('GAME LENGTH (TURNS)')
    print('=' * 60)
    df = pd.read_sql(f'''
        SELECT
            CASE
                WHEN Turns <= 5 THEN '1-5'
                WHEN Turns <= 8 THEN '6-8'
                WHEN Turns <= 12 THEN '9-12'
                ELSE '13+'
            END as Turns,
            COUNT(*) as Games,
            SUM(CASE WHEN Game_Winner = 'P1' THEN 1 ELSE 0 END) as Wins
        FROM Games
        WHERE P1 = '{hero}' AND Turns > 0
        GROUP BY CASE
                WHEN Turns <= 5 THEN '1-5'
                WHEN Turns <= 8 THEN '6-8'
                WHEN Turns <= 12 THEN '9-12'
                ELSE '13+'
            END
        ORDER BY MIN(Turns)
    ''', conn)
    df['Win%'] = (df['Wins'] / df['Games'] * 100).round(1)
    print(df.to_string(index=False))

    conn.close()

if __name__ == '__main__':
    main()
