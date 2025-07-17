import gspread
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import logging
import os
import requests
import json
from typing import Dict, List
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KahootLeaderboardDashboard:
    def __init__(self, credentials_path: str = None, sheet_id: str = None):
        """
        Initialize the Kahoot Leaderboard Dashboard
        
        Args:
            credentials_path: Path to the service account JSON file (optional, uses env var if not provided)
            sheet_id: Google Sheets ID (optional, uses env var if not provided)
        """
        self.credentials_path = credentials_path or os.getenv('GOOGLE_CREDENTIALS_PATH', 'pullus-pipeline-40a5302e034d.json')
        self.sheet_id = sheet_id or os.getenv('GOOGLE_SHEET_ID')
        
        if not self.sheet_id:
            raise ValueError("Google Sheet ID must be provided either as parameter or GOOGLE_SHEET_ID environment variable")
        self.gc = None
        self.workbook = None
        self.data_sheet = None
        self.viz_sheet = None
        
        # Dashboard colors (professional eye-friendly palette)
        self.colors = {
            'header_bg': {'red': 0.15, 'green': 0.35, 'blue': 0.75},    # Professional blue
            'header_text': {'red': 1, 'green': 1, 'blue': 1},           # White
            'rank_1': {'red': 1, 'green': 0.84, 'blue': 0},             # Pure gold
            'rank_2': {'red': 0.85, 'green': 0.85, 'blue': 0.85},      # Bright silver
            'rank_3': {'red': 0.9, 'green': 0.55, 'blue': 0.25},       # Rich bronze
            'alternating_bg': {'red': 0.98, 'green': 0.98, 'blue': 0.98}, # Very light gray
            'text_dark': {'red': 0.15, 'green': 0.15, 'blue': 0.15},   # Darker text
            'accent': {'red': 0.2, 'green': 0.65, 'blue': 0.45},       # Professional green
            'summary_bg': {'red': 0.25, 'green': 0.6, 'blue': 0.8}     # Light blue for summary
        }
        
        self.connect_to_sheets()
    
    def connect_to_sheets(self):
        """Connect to Google Sheets using service account credentials"""
        try:
            # Handle credentials from environment variable or file
            if os.getenv('GOOGLE_CREDENTIALS_JSON'):
                # Use credentials from environment variable (for GitHub Actions)
                credentials_info = json.loads(os.getenv('GOOGLE_CREDENTIALS_JSON'))
                self.gc = gspread.service_account_from_dict(credentials_info)
            else:
                # Use credentials from file (for local development)
                self.gc = gspread.service_account(filename=self.credentials_path)
            self.workbook = self.gc.open_by_key(self.sheet_id)
            
            # Get or create sheets
            try:
                self.data_sheet = self.workbook.worksheet('Data')
            except gspread.WorksheetNotFound:
                logger.error("'Data' sheet not found")
                raise
            
            try:
                self.viz_sheet = self.workbook.worksheet('Viz')
            except gspread.WorksheetNotFound:
                logger.info("'Viz' sheet not found, creating it...")
                self.viz_sheet = self.workbook.add_worksheet(title='Viz', rows=50, cols=10)
            
            logger.info("Successfully connected to Google Sheets")
            
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {e}")
            raise
    
    def read_data(self) -> pd.DataFrame:
        """Read data from the 'data' sheet and return as DataFrame"""
        try:
            # Get all values from data sheet
            values = self.data_sheet.get_all_values()
            
            if not values:
                logger.warning("No data found in sheet")
                return pd.DataFrame()
            
            # Convert to DataFrame with proper column names
            # The actual structure is: DATE, 1ST PLACE, SCORE, 2ND PLACE, SCORE, 3RD PLACE, SCORE
            column_names = ['DATE', '1ST_PLACE', 'SCORE_1ST', '2ND_PLACE', 'SCORE_2ND', '3RD_PLACE', 'SCORE_3RD']
            
            df = pd.DataFrame(values[1:], columns=column_names)  # Skip header row
            
            # Clean and convert data types
            df['DATE'] = pd.to_datetime(df['DATE'], format='%d-%b-%Y', errors='coerce')
            
            # Convert score columns to numeric, handling empty strings
            score_columns = ['SCORE_1ST', 'SCORE_2ND', 'SCORE_3RD']
            for col in score_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            logger.info(f"Read {len(df)} rows of data")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read data: {e}")
            return pd.DataFrame()
    
    def calculate_leaderboard(self, df: pd.DataFrame) -> List[Dict]:
        """Calculate leaderboard from the data"""
        try:
            leaderboard = []
            
            # Process each row to extract player scores
            for _, row in df.iterrows():
                date = row['DATE']
                
                # Extract players and scores for each position
                positions = [
                    ('1ST_PLACE', 'SCORE_1ST', '1ST PLACE'),
                    ('2ND_PLACE', 'SCORE_2ND', '2ND PLACE'),
                    ('3RD_PLACE', 'SCORE_3RD', '3RD PLACE')
                ]
                
                for pos_name, score_col, display_pos in positions:
                    if pos_name in row and score_col in row:
                        player = str(row[pos_name]).strip()
                        score = row[score_col]
                        
                        if player and player != '' and score > 0:
                            leaderboard.append({
                                'player': player,
                                'score': score,
                                'date': date,
                                'position': display_pos
                            })
            
            # Group by player and calculate stats
            player_stats = defaultdict(lambda: {'total_score': 0, 'games_played': 0, 'best_score': 0, 'positions': []})
            
            for entry in leaderboard:
                player = entry['player']
                score = entry['score']
                
                player_stats[player]['total_score'] += score
                player_stats[player]['games_played'] += 1
                player_stats[player]['best_score'] = max(player_stats[player]['best_score'], score)
                player_stats[player]['positions'].append(entry['position'])
            
            # Create final leaderboard
            final_leaderboard = []
            for player, stats in player_stats.items():
                avg_score = stats['total_score'] / stats['games_played']
                final_leaderboard.append({
                    'player': player,
                    'total_score': stats['total_score'],
                    'games_played': stats['games_played'],
                    'best_score': stats['best_score'],
                    'avg_score': avg_score,
                    'positions': stats['positions']
                })
            
            # Sort by total score (descending)
            final_leaderboard.sort(key=lambda x: x['total_score'], reverse=True)
            
            logger.info(f"Calculated leaderboard for {len(final_leaderboard)} players")
            return final_leaderboard
            
        except Exception as e:
            logger.error(f"Failed to calculate leaderboard: {e}")
            return []
    
    def format_cell(self, worksheet, cell_range: str, bg_color: Dict, text_color: Dict, 
                   bold: bool = False, font_size: int = 10, horizontal_alignment: str = 'LEFT'):
        """Format a cell or range of cells"""
        try:
            worksheet.format(cell_range, {
                'backgroundColor': bg_color,
                'textFormat': {
                    'foregroundColor': text_color,
                    'bold': bold,
                    'fontSize': font_size
                },
                'horizontalAlignment': horizontal_alignment,
                'verticalAlignment': 'MIDDLE'
            })
        except Exception as e:
            logger.error(f"Failed to format cell {cell_range}: {e}")
    
    def create_dashboard(self, leaderboard: List[Dict], df, last_data_date: str = None):
        """Create the dashboard on the Viz sheet"""
        try:
            # Clear existing content
            self.viz_sheet.clear()
            
            # Dashboard title and timestamp
            if last_data_date:
                last_updated = f"Last Game: {last_data_date}"
            else:
                last_updated = f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Title
            self.viz_sheet.update('A1', [['KAHOOT GAMES LEADERBOARD']])
            self.format_cell(self.viz_sheet, 'A1:H1', self.colors['header_bg'], 
                           self.colors['header_text'], bold=True, font_size=16, 
                           horizontal_alignment='CENTER')
            
            # Merge title cells
            self.viz_sheet.merge_cells('A1:H1')
            
            # Last updated
            self.viz_sheet.update('A2', [[last_updated]])
            self.format_cell(self.viz_sheet, 'A2:H2', self.colors['alternating_bg'], 
                           self.colors['text_dark'], font_size=10, horizontal_alignment='CENTER')
            self.viz_sheet.merge_cells('A2:H2')
            
            # Headers
            headers = ['RANK', 'PLAYER', 'TOTAL SCORE', 'TOP 3 FINISHES', 'BEST SCORE', 'AVG SCORE', 'WIN RATE', 'BADGES']
            self.viz_sheet.update('A4:H4', [headers])
            self.format_cell(self.viz_sheet, 'A4:H4', self.colors['header_bg'], 
                           self.colors['header_text'], bold=True, font_size=12, 
                           horizontal_alignment='CENTER')
            
            # Data rows
            data_rows = []
            for i, player_data in enumerate(leaderboard[:10]):  # Top 10 players
                rank = i + 1
                
                # Calculate win rate (1st place finishes / total top 3 finishes)
                first_place_count = player_data['positions'].count('1ST PLACE')
                win_rate = (first_place_count / player_data['games_played']) * 100
                
                # Badges based on performance (clean emoji format)
                badges = []
                if rank == 1:
                    badges.append('🏆')
                elif rank == 2:
                    badges.append('🥈')
                elif rank == 3:
                    badges.append('🥉')
                
                if first_place_count >= 2:
                    badges.append('🔥')
                if player_data['best_score'] >= 9000:
                    badges.append('⭐')
                
                badge_text = ' '.join(badges) if badges else ''
                
                row_data = [
                    rank,
                    player_data['player'],
                    f"{int(player_data['total_score']):,}",
                    player_data['games_played'],
                    f"{int(player_data['best_score']):,}",
                    f"{player_data['avg_score']:,.1f}",
                    f"{win_rate:.1f}%",
                    badge_text
                ]
                data_rows.append(row_data)
            
            # Update data
            if data_rows:
                self.viz_sheet.update(f'A5:H{4 + len(data_rows)}', data_rows)
                
                # Format data rows
                for i, row in enumerate(data_rows):
                    row_num = 5 + i
                    rank = row[0]
                    
                    # Special formatting for top 3
                    if rank == 1:
                        bg_color = self.colors['rank_1']
                    elif rank == 2:
                        bg_color = self.colors['rank_2']
                    elif rank == 3:
                        bg_color = self.colors['rank_3']
                    else:
                        bg_color = self.colors['alternating_bg'] if i % 2 == 0 else {'red': 1, 'green': 1, 'blue': 1}
                    
                    self.format_cell(self.viz_sheet, f'A{row_num}:H{row_num}', 
                                   bg_color, self.colors['text_dark'], 
                                   bold=(rank <= 3), font_size=11, 
                                   horizontal_alignment='CENTER')
            
            # Summary statistics
            summary_row = 4 + len(data_rows) + 2
            self.viz_sheet.update(f'A{summary_row}', [['SUMMARY STATISTICS']])
            self.format_cell(self.viz_sheet, f'A{summary_row}:H{summary_row}', 
                           self.colors['summary_bg'], self.colors['header_text'], 
                           bold=True, font_size=14, horizontal_alignment='CENTER')
            self.viz_sheet.merge_cells(f'A{summary_row}:H{summary_row}')
            
            # Summary data
            total_players = len(leaderboard)
            total_games = len(df) if not df.empty else 0
            if leaderboard:
                highest_score = max(player['best_score'] for player in leaderboard)
                avg_score_all = sum(player['avg_score'] for player in leaderboard) / len(leaderboard)
                current_leader = leaderboard[0]['player']
            else:
                highest_score = 0
                avg_score_all = 0
                current_leader = "No data"
            
            summary_data = [
                ['Top 3 Finishers:', total_players],
                ['Total Games Played:', total_games],
                ['Current Leader:', current_leader],
                ['Highest Score Ever:', f"{int(highest_score):,}"],
                ['Average Score (Top 3):', f"{avg_score_all:,.1f}"]
            ]
            
            summary_start = summary_row + 1
            self.viz_sheet.update(f'A{summary_start}:B{summary_start + len(summary_data) - 1}', summary_data)
            
            # Format summary
            for i in range(len(summary_data)):
                row_num = summary_start + i
                self.format_cell(self.viz_sheet, f'A{row_num}:B{row_num}', 
                               self.colors['alternating_bg'], self.colors['text_dark'], 
                               font_size=11)
            
            # Set fixed column widths to prevent shrinking
            try:
                requests = []
                column_widths = [
                    (0, 160),  # RANK (widened to accommodate summary labels)
                    (1, 200),  # PLAYER
                    (2, 120),  # TOTAL SCORE
                    (3, 130),  # TOP 3 FINISHES
                    (4, 120),  # BEST SCORE
                    (5, 110),  # AVG SCORE
                    (6, 100),  # WIN RATE
                    (7, 120)   # BADGES
                ]
                
                for col_index, width in column_widths:
                    requests.append({
                        'updateDimensionProperties': {
                            'range': {
                                'sheetId': self.viz_sheet.id,
                                'dimension': 'COLUMNS',
                                'startIndex': col_index,
                                'endIndex': col_index + 1
                            },
                            'properties': {
                                'pixelSize': width
                            },
                            'fields': 'pixelSize'
                        }
                    })
                
                # Execute all column width updates at once
                self.workbook.batch_update({'requests': requests})
                
            except Exception as e:
                logger.warning(f"Could not set column widths: {e}")
            
            logger.info("Dashboard created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise
    
    def refresh_dashboard(self):
        """Refresh the dashboard with latest data"""
        try:
            logger.info("Refreshing dashboard...")
            
            # Read latest data
            df = self.read_data()
            if df.empty:
                logger.warning("No data available for dashboard")
                return
            
            # Get the most recent game date
            last_data_date = None
            if 'DATE' in df.columns and not df['DATE'].isna().all():
                last_data_date = df['DATE'].max().strftime('%d-%b-%Y')
            
            # Calculate leaderboard
            leaderboard = self.calculate_leaderboard(df)
            if not leaderboard:
                logger.warning("No leaderboard data calculated")
                return
            
            # Create dashboard
            self.create_dashboard(leaderboard, df, last_data_date)
            
            logger.info("Dashboard refreshed successfully")
            
        except Exception as e:
            logger.error(f"Failed to refresh dashboard: {e}")
            raise
    
    def auto_refresh(self, interval_seconds: int = 300):  # 5 minutes default
        """Automatically refresh dashboard at specified intervals"""
        logger.info(f"Starting auto-refresh with {interval_seconds} second intervals")
        
        while True:
            try:
                self.refresh_dashboard()
                logger.info(f"Next refresh in {interval_seconds} seconds...")
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("Auto-refresh stopped by user")
                break
            except Exception as e:
                logger.error(f"Error during auto-refresh: {e}")
                logger.info(f"Retrying in {interval_seconds} seconds...")
                time.sleep(interval_seconds)
    
    def should_send_alert(self):
        """Check if it's time to send the bi-weekly alert (Monday 5pm WAT, starting July 21st, 2025)"""
        try:
            # Get current time in WAT (UTC+1)
            wat_timezone = timezone(timedelta(hours=1))  # WAT is UTC+1
            wat_now = datetime.now(wat_timezone)  # Current time in WAT
            
            # Starting date: July 21st, 2025 at 5pm WAT
            start_date = datetime(2025, 7, 21, 17, 0, 0, tzinfo=wat_timezone)  # 5pm WAT
            
            # Check if we're past the start date
            if wat_now < start_date:
                return False
            
            # Check if it's Monday (weekday 0) and 5pm WAT
            if wat_now.weekday() != 0 or wat_now.hour != 17:
                return False
            
            # Check if it's within the 5pm hour (17:00-17:59)
            if not (0 <= wat_now.minute < 60):
                return False
            
            # Calculate weeks since start date
            days_since_start = (wat_now.date() - start_date.date()).days
            weeks_since_start = days_since_start // 7
            
            # Send alert every 2 weeks (even weeks: 0, 2, 4, 6...)
            return weeks_since_start % 2 == 0
            
        except Exception as e:
            logger.error(f"Error checking alert schedule: {e}")
            return False
    
    def send_google_chat_alert(self, leaderboard: List[Dict]):
        """Send top 3 leaderboard to Google Chat"""
        try:
            webhook_url = os.getenv('GOOGLE_CHAT_WEBHOOK_URL')
            if not webhook_url:
                logger.warning("Google Chat webhook URL not configured")
                return
            
            if not leaderboard:
                logger.warning("No leaderboard data to send")
                return
            
            # Get top 3 players
            top_3 = leaderboard[:3]
            
            # Create message
            message = {
                "text": f"🏆 *KAHOOT LEADERBOARD UPDATE* 🏆\n\n"
                       f"📊 *Top 3 Performers:*\n\n"
            }
            
            # Add top 3 players
            badges = ['🥇', '🥈', '🥉']
            for i, player in enumerate(top_3):
                badge = badges[i]
                message["text"] += (
                    f"{badge} *{player['player']}* - {player['total_score']:,} points\n"
                    f"   • {player['games_played']} top 3 finishes\n"
                    f"   • Best score: {player['best_score']:,}\n"
                    f"   • Win rate: {(player['positions'].count('1ST PLACE') / player['games_played']) * 100:.1f}%\n\n"
                )
            
            message["text"] += f"📈 *Total Games Played:* {len(self.read_data())}\n"
            message["text"] += f"🎯 *Keep up the great work, team!*"
            
            # Send to Google Chat
            response = requests.post(
                webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info("Google Chat alert sent successfully")
            else:
                logger.error(f"Failed to send Google Chat alert: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error sending Google Chat alert: {e}")
    
    def run_scheduled_tasks(self):
        """Run dashboard update and check for scheduled alerts"""
        try:
            # Always refresh the dashboard
            self.refresh_dashboard()
            
            # Check if we should send alert
            if self.should_send_alert():
                logger.info("Sending bi-weekly Google Chat alert...")
                df = self.read_data()
                if not df.empty:
                    leaderboard = self.calculate_leaderboard(df)
                    self.send_google_chat_alert(leaderboard)
                else:
                    logger.warning("No data available for alert")
            else:
                logger.info("Not time for bi-weekly alert")
                
        except Exception as e:
            logger.error(f"Error in scheduled tasks: {e}")
            raise

def main():
    """Main function to run the dashboard"""
    try:
        # Check if running in GitHub Actions or scheduled mode
        if os.getenv('GITHUB_ACTIONS') or os.getenv('SCHEDULED_RUN'):
            # Running in GitHub Actions - use environment variables and run scheduled tasks
            dashboard = KahootLeaderboardDashboard()
            dashboard.run_scheduled_tasks()
            return
        
        # Local development mode - use interactive interface
        dashboard = KahootLeaderboardDashboard()
        
        # Initial refresh
        dashboard.refresh_dashboard()
        
        # Ask user if they want auto-refresh
        print("\n" + "="*60)
        print("KAHOOT LEADERBOARD DASHBOARD")
        print("="*60)
        print("Dashboard created successfully!")
        print("\nOptions:")
        print("1. One-time refresh (default)")
        print("2. Auto-refresh every 5 minutes")
        print("3. Auto-refresh with custom interval")
        print("4. Run scheduled tasks (test mode)")
        
        choice = input("\nEnter your choice (1-4): ").strip() or "1"
        
        if choice == "2":
            dashboard.auto_refresh(300)  # 5 minutes
        elif choice == "3":
            try:
                interval = int(input("Enter refresh interval in seconds: "))
                dashboard.auto_refresh(interval)
            except ValueError:
                print("Invalid interval. Using one-time refresh.")
        elif choice == "4":
            dashboard.run_scheduled_tasks()
        else:
            print("One-time refresh completed. Run the script again to refresh manually.")
            
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()