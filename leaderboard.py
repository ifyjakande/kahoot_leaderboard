import gspread
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import logging
import os
import requests
import json
from typing import Dict, List

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
                self.data_sheet = self.workbook.worksheet('Team')
            except gspread.WorksheetNotFound:
                logger.error("'Team' sheet not found")
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
        """Read data from the 'Team' sheet and return as DataFrame"""
        try:
            # Get all values from team sheet
            values = self.data_sheet.get_all_values()
            
            if not values:
                logger.warning("No data found in sheet")
                return pd.DataFrame()
            
            # Convert to DataFrame - Team sheet structure: Name, Date1, Date2, Date3, ...
            df = pd.DataFrame(values[1:], columns=values[0])  # Use first row as headers
            
            if 'Name' not in df.columns:
                logger.error("'Name' column not found in Team sheet")
                return pd.DataFrame()
            
            # Get date columns (all columns except 'Name')
            date_columns = [col for col in df.columns if col != 'Name']
            
            # Convert score columns to numeric, handling empty strings and zeros
            for col in date_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Remove rows where Name is empty
            df = df[df['Name'].str.strip() != '']
            
            logger.info(f"Read {len(df)} players with {len(date_columns)} game dates")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read data: {e}")
            return pd.DataFrame()
    
    def calculate_leaderboard(self, df: pd.DataFrame) -> List[Dict]:
        """Calculate leaderboard from the Team sheet data"""
        try:
            if df.empty:
                return []
            
            # Get date columns (all columns except 'Name')
            date_columns = [col for col in df.columns if col != 'Name']
            
            if not date_columns:
                logger.warning("No date columns found in data")
                return []
            
            final_leaderboard = []
            
            # Process each player
            for _, row in df.iterrows():
                player_name = str(row['Name']).strip()
                
                if not player_name:
                    continue
                
                # Calculate player statistics
                player_scores = []
                total_score = 0
                games_played = 0
                best_score = 0
                first_place_count = 0
                
                # Get scores for each game date
                for date_col in date_columns:
                    score = row[date_col]
                    if score > 0:  # Only count non-zero scores as games played
                        player_scores.append((date_col, score))
                        total_score += score
                        games_played += 1
                        best_score = max(best_score, score)
                
                # Calculate positions for each game (determine how many times they came 1st)
                for date_col in date_columns:
                    player_score = row[date_col]
                    if player_score > 0:
                        # Get all players' scores for this date to determine position
                        date_scores = df[df[date_col] > 0][date_col].tolist()
                        date_scores.sort(reverse=True)
                        
                        # Check if this player got 1st place in this game
                        if date_scores and player_score == date_scores[0]:
                            # Handle ties - only count as 1st if they're the only one with the highest score
                            if date_scores.count(player_score) == 1:
                                first_place_count += 1
                
                # Only include players who have played at least one game
                if games_played > 0:
                    avg_score = total_score / games_played
                    win_rate = (first_place_count / games_played) * 100
                    
                    # Create positions list for compatibility (simplified)
                    positions = ['1ST PLACE'] * first_place_count + ['PARTICIPATED'] * (games_played - first_place_count)
                    
                    final_leaderboard.append({
                        'player': player_name,
                        'total_score': total_score,
                        'games_played': games_played,
                        'best_score': best_score,
                        'avg_score': avg_score,
                        'win_rate': win_rate,
                        'first_place_count': first_place_count,
                        'positions': positions,
                        'player_scores': player_scores
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
                # Fallback to current time in WAT with 12-hour format
                wat_timezone = timezone(timedelta(hours=1))
                current_time_wat = datetime.now(wat_timezone)
                last_updated = f"Last Updated: {current_time_wat.strftime('%d-%b-%Y %I:%M:%S %p WAT')}"
            
            # Title
            self.viz_sheet.update(values=[['KAHOOT GAMES LEADERBOARD']], range_name='A1')
            self.format_cell(self.viz_sheet, 'A1:H1', self.colors['header_bg'], 
                           self.colors['header_text'], bold=True, font_size=16, 
                           horizontal_alignment='CENTER')
            
            # Merge title cells
            self.viz_sheet.merge_cells('A1:H1')
            
            # Last updated
            self.viz_sheet.update(values=[[last_updated]], range_name='A2')
            self.format_cell(self.viz_sheet, 'A2:H2', self.colors['alternating_bg'], 
                           self.colors['text_dark'], font_size=10, horizontal_alignment='CENTER')
            self.viz_sheet.merge_cells('A2:H2')
            
            # Headers
            headers = ['RANK', 'PLAYER', 'TOTAL SCORE', 'GAMES PLAYED', 'BEST SCORE', 'AVG SCORE', 'WIN RATE', 'BADGES']
            self.viz_sheet.update(values=[headers], range_name='A4:H4')
            self.format_cell(self.viz_sheet, 'A4:H4', self.colors['header_bg'], 
                           self.colors['header_text'], bold=True, font_size=12, 
                           horizontal_alignment='CENTER')
            
            # Data rows
            data_rows = []
            for i, player_data in enumerate(leaderboard):  # All players
                rank = i + 1
                
                # Get win rate from calculated data
                first_place_count = player_data.get('first_place_count', 0)
                win_rate = player_data.get('win_rate', 0)
                
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
                self.viz_sheet.update(values=data_rows, range_name=f'A5:H{4 + len(data_rows)}')
                
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
            self.viz_sheet.update(values=[['SUMMARY STATISTICS']], range_name=f'A{summary_row}')
            self.format_cell(self.viz_sheet, f'A{summary_row}:H{summary_row}', 
                           self.colors['summary_bg'], self.colors['header_text'], 
                           bold=True, font_size=14, horizontal_alignment='CENTER')
            self.viz_sheet.merge_cells(f'A{summary_row}:H{summary_row}')
            
            # Summary data
            total_players = len(leaderboard)
            
            # Calculate total unique games from date columns
            date_columns = [col for col in df.columns if col != 'Name'] if not df.empty else []
            total_games = len(date_columns)
            
            if leaderboard:
                highest_score = max(player['best_score'] for player in leaderboard)
                # Find the player with the highest score
                highest_score_player = max(leaderboard, key=lambda x: x['best_score'])['player']
                avg_score_all = sum(player['avg_score'] for player in leaderboard) / len(leaderboard)
                current_leader = leaderboard[0]['player']
                total_participations = sum(player['games_played'] for player in leaderboard)
            else:
                highest_score = 0
                highest_score_player = "No data"
                avg_score_all = 0
                current_leader = "No data"
                total_participations = 0
            
            summary_data = [
                ['Total Players:', total_players],
                ['Total Games Played:', total_games],
                ['Current Leader:', current_leader],
                ['Highest Score Ever:', f"{int(highest_score):,} ({highest_score_player})"],
                ['Average Score (All Players):', f"{avg_score_all:,.1f}"],
                ['Total Participations:', total_participations]
            ]
            
            summary_start = summary_row + 1
            self.viz_sheet.update(values=summary_data, range_name=f'A{summary_start}:B{summary_start + len(summary_data) - 1}')
            
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
                    (0, 180),  # RANK (wider for summary labels and 3-digit ranks)
                    (1, 250),  # PLAYER (wider for longer names)
                    (2, 140),  # TOTAL SCORE (wider for large numbers with commas)
                    (3, 140),  # GAMES PLAYED (consistent with scores)
                    (4, 140),  # BEST SCORE (wider for large numbers with commas)
                    (5, 120),  # AVG SCORE (sufficient for decimal numbers)
                    (6, 110),  # WIN RATE (sufficient for percentages)
                    (7, 140)   # BADGES (wider for multiple emoji badges)
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
                
                # Add row height adjustments for better display
                # Set header row height
                requests.append({
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': self.viz_sheet.id,
                            'dimension': 'ROWS',
                            'startIndex': 3,  # Row 4 (headers)
                            'endIndex': 4
                        },
                        'properties': {
                            'pixelSize': 35  # Taller header row
                        },
                        'fields': 'pixelSize'
                    }
                })
                
                # Set data rows height (if there are data rows)
                if data_rows:
                    requests.append({
                        'updateDimensionProperties': {
                            'range': {
                                'sheetId': self.viz_sheet.id,
                                'dimension': 'ROWS',
                                'startIndex': 4,  # Row 5 onwards (data rows)
                                'endIndex': 4 + len(data_rows)
                            },
                            'properties': {
                                'pixelSize': 28  # Optimal height for data rows
                            },
                            'fields': 'pixelSize'
                        }
                    })
                
                # Execute all dimension updates at once
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
            
            # Get the most recent game date from column headers
            last_data_date = None
            date_columns = [col for col in df.columns if col != 'Name']
            if date_columns:
                # Get the last date column (assuming they're in chronological order)
                last_date_str = date_columns[-1]
                try:
                    # Parse the date and format it (date only, no time)
                    from datetime import datetime
                    last_date = datetime.strptime(last_date_str, '%d-%b-%Y')
                    
                    # Format as date only
                    last_data_date = last_date.strftime('%d-%b-%Y')
                except ValueError:
                    # Fallback to just the date string if parsing fails
                    last_data_date = last_date_str
            
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
                win_rate = player.get('win_rate', 0)
                message["text"] += (
                    f"{badge} *{player['player']}* - {player['total_score']:,} points\n"
                    f"   • {player['games_played']} games played\n"
                    f"   • Best score: {player['best_score']:,}\n"
                    f"   • Win rate: {win_rate:.1f}%\n\n"
                )
            
            # Calculate actual number of games (date columns)
            df = self.read_data()
            date_columns = [col for col in df.columns if col != 'Name'] if not df.empty else []
            total_games = len(date_columns)
            
            message["text"] += f"📈 *Total Games Played:* {total_games}\n"
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
                logger.error(f"Failed to send Google Chat alert: HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error sending Google Chat alert: {e}")
    
    def run_scheduled_tasks(self):
        """Run dashboard update and check for scheduled alerts on bi-weekly schedule"""
        try:
            # Check for manual trigger override
            is_manual_trigger = os.getenv('MANUAL_TRIGGER', '').lower() == 'true'
            
            if is_manual_trigger:
                logger.info("Manual trigger detected - running dashboard update and alert for testing...")
                
                # Always run for manual triggers (testing mode)
                self.refresh_dashboard()
                
                # Send the alert
                df = self.read_data()
                if not df.empty:
                    leaderboard = self.calculate_leaderboard(df)
                    self.send_google_chat_alert(leaderboard)
                else:
                    logger.warning("No data available for alert")
                    
            elif self.should_send_alert():
                logger.info("Bi-weekly schedule - updating dashboard and sending alert...")
                
                # Refresh the dashboard
                self.refresh_dashboard()
                
                # Send the alert
                df = self.read_data()
                if not df.empty:
                    leaderboard = self.calculate_leaderboard(df)
                    self.send_google_chat_alert(leaderboard)
                else:
                    logger.warning("No data available for alert")
            else:
                logger.info("Not time for bi-weekly update - skipping dashboard refresh and alert")
                
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