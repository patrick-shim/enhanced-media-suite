# deduper_modules_db.py

import re
from datetime import datetime
import pyodbc
from typing import List, Dict, Tuple
from collections import defaultdict, deque

from source.logging_modules import CustomLogger

class Deduper:
    """
    High-level class that uses:
      - DatabaseManager to read/write to the DB
      - Uses pre-calculated hash values from the source table
      - Provides directory-based deduplication with representatives
      - Supports single-phase or two-phase algorithms
    """

    def __init__(self, db_manager, deduplicator=None):
        """
        :param db_manager: Instance of your DatabaseManager
        :param deduplicator: Optional deduplicator instance (not used anymore)
        """
        self.db_manager = db_manager
        self.logger = CustomLogger(__name__).get_logger()

    # --------------------------------------------------------------------------
    # Single-phase dedup by DIRECTORY using pre-calculated hashes from DB
    # --------------------------------------------------------------------------
    def dedupe_by_hash(
        self,
        connection: pyodbc.Connection,
        source_table: str,
        method: str = 'phash',
        distance_threshold: int = 10,
        target_table: str = None
    ) -> str:
        """
        1) Load entire table into memory
        2) Create new table named <source_table>_deduped_<method> (or user-specified target)
        3) Process each directory separately:
           a. Cluster images by hamming distance using the pre-calculated hash values
           b. Mark one representative per cluster
           c. Mark all videos as representatives
        4) Insert all rows with is_representative flag
        5) Return the name of the created table
        """
        rows = self._load_table_into_memory(connection, source_table)
        if target_table is None:
            target_table = f"{source_table}_deduped_{method}"
            
        # Always drop and recreate the target table
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black] Dropping target table if it exists: [bold cyan]{target_table}[/bold cyan]")
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS [dbo].[{target_table}];")
        connection.commit()
        
        self._create_deduped_table_with_is_representative(connection, source_table, target_table)

        # Group by directory
        dir_map = defaultdict(list)
        for r in rows:
            dir_map[r.file_directory].append(r)

        # Track which file_path => is_representative
        is_rep_map = {}

        # Process each directory separately
        for dir_key, dir_rows in dir_map.items():
            self.logger.info(f"[bright_black][Deduper]📸[/bright_black] Processing directory: [bold cyan]{dir_key}[/bold cyan] ({len(dir_rows)} files)")
            
            # Separate images vs videos
            image_rows = [r for r in dir_rows if r.file_type.lower().startswith("image")]
            video_rows = [r for r in dir_rows if not r.file_type.lower().startswith("image")]

            # Cluster using the pre-calculated hash values from DB
            clusters = self._cluster_by_hash(image_rows, method, distance_threshold)

            # Pick 1 rep per cluster
            for cluster in clusters:
                if not cluster:
                    continue
                #rep = cluster[0]  # Simple: pick the first
                rep = max(cluster, key=lambda row: self._get_representative_score(row))
                for row in cluster:
                    is_rep_map[row.file_path] = (row.file_path == rep.file_path)

            # Mark all videos as representatives
            for v in video_rows:
                is_rep_map[v.file_path] = True

        # Insert all rows with their is_representative flag
        insert_count = 0
        cur = connection.cursor()

        for row in rows:
            fp = row.file_path
            rep_val = is_rep_map.get(fp, True)  # Default to True for anything not processed

            col_names, col_values = self._extract_row_data(row)
            col_names.append("is_representative")
            col_names.append("dedupe_method")
            col_values.append(1 if rep_val else 0)
            col_values.append(method)

            placeholders = ", ".join(["?"] * len(col_names))
            col_str = ", ".join(f"[{cn}]" for cn in col_names)
            sql = f"INSERT INTO [dbo].[{target_table}] ({col_str}) VALUES ({placeholders});"
            try:
                cur.execute(sql, col_values)
                insert_count += 1
            except Exception as e:
                connection.rollback()
                self.logger.error(f"[bright_black][Deduper]📸[/bright_black][bold red] Error inserting row for {fp}: {e}[/bold red]")
                continue

        connection.commit()
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black][bold #FFA500] Inserted {insert_count} rows into {target_table}[/bold #FFA500]")
        
        # Log summary of representatives
        rep_count = sum(1 for val in is_rep_map.values() if val)
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black][bold green] Total representatives: {rep_count}/{len(is_rep_map)}[/bold green]")
        
        return target_table

    # --------------------------------------------------------------------------
    # 2-phase dedup (dHash -> pHash) by DIRECTORY using pre-calculated hashes
    # --------------------------------------------------------------------------
    def dedupe_two_phase(
        self,
        connection: pyodbc.Connection,
        source_table: str,
        threshold_dhash: int = 10,
        threshold_phash: int = 8,
        target_table: str = None
    ) -> str:
        """
        1) Load entire table
        2) Create target table with is_representative column
        3) Process each directory separately:
           a. For each directory:
              i.   First cluster by dHash with threshold_dhash using pre-calculated hashes
              ii.  For each cluster found, refine it with pHash using threshold_phash
              iii. Mark one representative per final cluster
           b. Mark all videos as representatives
        4) Insert into new table, returning the new table name
        """
        rows = self._load_table_into_memory(connection, source_table)
        if target_table is None:
            target_table = f"{source_table}_deduped_dhash_phash"
            
        # Always drop and recreate the target table
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black] Dropping target table if it exists: [bold cyan]{target_table}[/bold cyan]")
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS [dbo].[{target_table}];")
        connection.commit()

        self._create_deduped_table_with_is_representative(connection, source_table, target_table)

        # Group by directory
        dir_map = defaultdict(list)
        for r in rows:
            dir_map[r.file_directory].append(r)

        # Track which file_path => is_representative
        is_rep_map = {}

        # Process each directory separately
        for dir_key, dir_rows in dir_map.items():
            self.logger.info(f"[bright_black][Deduper]📸[/bright_black] Processing directory: [bold cyan]{dir_key}[/bold cyan] ({len(dir_rows)} files)")
            
            # Separate images vs videos
            image_rows = [r for r in dir_rows if r.file_type.lower().startswith("image")]
            video_rows = [r for r in dir_rows if not r.file_type.lower().startswith("image")]

            # Phase 1: Cluster by dHash using pre-calculated hashes
            d_clusters = self._cluster_by_hash(image_rows, 'dhash', threshold_dhash)
            
            # Phase 2: Refine each dHash cluster with pHash
            final_clusters = []
            for d_cluster in d_clusters:
                if len(d_cluster) <= 1:
                    final_clusters.append(d_cluster)
                else:
                    p_sub = self._cluster_by_hash(d_cluster, 'phash', threshold_phash)
                    final_clusters.extend(p_sub)

            # Pick 1 rep per cluster
            for cluster in final_clusters:
                if not cluster:
                    continue
                # Pick the row with the highest representative score (if you want the first one, use cluster[0])
                rep = max(cluster, key=lambda row: self._get_representative_score(row))
                
                for row_obj in cluster:
                    is_rep_map[row_obj.file_path] = (row_obj.file_path == rep.file_path)

            # Mark all videos as representatives
            for v in video_rows:
                is_rep_map[v.file_path] = True

        # Now insert every row with its is_representative flag
        insert_count = 0
        cur = connection.cursor()

        for row in rows:
            fp = row.file_path
            rep_val = is_rep_map.get(fp, True)  # Default to True for anything not processed

            col_names, col_values = self._extract_row_data(row)
            col_names.append("is_representative")
            col_names.append("dedupe_phase1")
            col_names.append("dedupe_phase2")
            col_values.append(1 if rep_val else 0)
            col_values.append(f"dHash:{threshold_dhash}")
            col_values.append(f"pHash:{threshold_phash}")

            placeholders = ", ".join(["?"] * len(col_names))
            col_str = ", ".join(f"[{cn}]" for cn in col_names)
            sql = f"INSERT INTO [dbo].[{target_table}] ({col_str}) VALUES ({placeholders});"
            try:
                cur.execute(sql, col_values)
                insert_count += 1
            except Exception as e:
                connection.rollback()
                self.logger.error(f"[bright_black][Deduper]📸[/bright_black][bold red] Error inserting row for {fp}: {e}[/bold red]")
                continue

        connection.commit()
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black][bold #FFA500] Inserted {insert_count} rows into {target_table}[/bold #FFA500]")
        
        # Log summary of representatives
        rep_count = sum(1 for val in is_rep_map.values() if val)
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black][bold green] Total representatives: {rep_count}/{len(is_rep_map)}[/bold green]")
        
        return target_table

    def _get_representative_score(self, row) -> tuple:

        filename = row.file_name

        # 1) Instagram style check
        has_instagram_format = bool(re.search(r'_\w+-\w+_', filename))
        score_instagram = 1 if has_instagram_format else 0

        # 2) No (n).jpg
        has_paren_n = bool(re.search(r'\(\d+\)', filename))
        score_no_paren = 1 if not has_paren_n else 0

        # 3) resolution
        file_size_val = row.file_size if row.file_size else 0

        # 4) date
        # Assume row.date is a datetime; if not, parse or fallback
        try:
            date_val = row.date if isinstance(row.date, datetime) else datetime.strptime(str(row.date), '%Y-%m-%d %H:%M:%S')
        except:
            date_val = datetime.min

        return (score_instagram, score_no_paren, file_size_val, date_val)
    # --------------------------------------------------------------------------
    # Helpers
    # --------------------------------------------------------------------------
    def _cluster_by_hash(self, rows, hash_type='phash', threshold=10):
        """
        Cluster images based on pre-calculated hash values from the DB.
        Uses BFS to find connected components based on hamming distance.
        
        :param rows: List of database rows (with hash attributes)
        :param hash_type: The hash column to use ('phash', 'dhash', etc.)
        :param threshold: Maximum hamming distance to consider similar
        :return: List of clusters (each cluster is a list of row objects)
        """
        # Filter out rows with NULL hash values
        valid_rows = []
        row_to_idx = {}  # Map each row to an index
        
        for i, row in enumerate(rows):
            hash_val = getattr(row, hash_type)
            if hash_val is not None and hash_val.strip():
                valid_rows.append(row)
                row_to_idx[row.file_path] = i
        
        if not valid_rows:
            return []
            
        # Build adjacency list using hamming distance between the hash strings
        # Use row indices instead of row objects (which aren't hashable)
        adj = [[] for _ in range(len(valid_rows))]
        
        for i in range(len(valid_rows)):
            for j in range(i+1, len(valid_rows)):
                row1 = valid_rows[i]
                row2 = valid_rows[j]
                hash1 = getattr(row1, hash_type)
                hash2 = getattr(row2, hash_type)
                
                # Calculate hamming distance between hash strings
                # Make sure to compare just the minimum length if they differ
                min_len = min(len(hash1), len(hash2))
                dist = sum(c1 != c2 for c1, c2 in zip(hash1[:min_len], hash2[:min_len]))
                
                if dist <= threshold:
                    adj[i].append(j)
                    adj[j].append(i)
        
        # Find clusters using BFS
        visited = [False] * len(valid_rows)
        clusters = []
        
        for i in range(len(valid_rows)):
            if not visited[i]:
                cluster = []
                queue = deque([i])
                visited[i] = True
                
                while queue:
                    curr_idx = queue.popleft()
                    cluster.append(valid_rows[curr_idx])
                    
                    for neighbor_idx in adj[curr_idx]:
                        if not visited[neighbor_idx]:
                            visited[neighbor_idx] = True
                            queue.append(neighbor_idx)
                            
                clusters.append(cluster)
        
        return clusters
        
    def _load_table_into_memory(self, connection, table_name: str):
        """
        Load entire table into memory.
        """
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black] Loading table [bold cyan]{table_name}[/bold cyan] into memory...")
        query = f"SELECT * FROM [dbo].[{table_name}];"
        return self.db_manager.fetch(connection, query)

    def _create_deduped_table_with_is_representative(self, connection, source_table, target_table):
        """
        Create a new table with same columns as source, plus
        [is_representative] and optional method columns.
        """
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black] Creating deduped table: [bold cyan]{target_table}[/bold cyan]")
        cursor = connection.cursor()

        create_sql = f"""
        CREATE TABLE [dbo].[{target_table}] (
            [file_path]         NVARCHAR(1024) NOT NULL,
            [file_name]         NVARCHAR(256)  NOT NULL,
            [file_directory]    NVARCHAR(128)  NOT NULL,
            [file_type]         NVARCHAR(64)   NOT NULL,
            [file_extension]    NVARCHAR(8)    NOT NULL,
            [file_size]         BIGINT         NOT NULL,
            [md5]               CHAR(64)       NULL,
            [sha256]            CHAR(256)      NULL,
            [sha512]            CHAR(512)      NULL,
            [blake3]            CHAR(512)      NOT NULL,
            [dhash]             CHAR(256)      NULL,
            [phash]             CHAR(256)      NULL,
            [whash]             CHAR(256)      NULL,
            [chash]             CHAR(256)      NULL,
            [ahash]             CHAR(256)      NULL,
            [video_fingerprint] CHAR(512)      NULL,
            [video_width]       INT            NULL,
            [video_height]      INT            NULL,
            [video_resolution]  VARCHAR(16)    NULL,
            [video_fps]         INT            NULL,
            [video_length]      FLOAT          NULL,
            [has_human]         BIT            NOT NULL DEFAULT 0,
            [has_human_score]   DECIMAL(5, 2)  NULL,
            [has_human_count]   INT            NULL,
            [date]              DATETIME       NOT NULL DEFAULT GETDATE(),

            -- Required columns for all deduplication strategies
            [is_representative] BIT            NOT NULL DEFAULT 0,
            
            -- Optional method columns
            [dedupe_method]     NVARCHAR(64)   NULL,
            [dedupe_phase1]     NVARCHAR(64)   NULL,
            [dedupe_phase2]     NVARCHAR(64)   NULL
        );
        """
        cursor.execute(create_sql)
        # replicate constraints
        cursor.execute(f"""
            ALTER TABLE [dbo].[{target_table}]
            ADD CONSTRAINT [UQ_{target_table}_blake3] UNIQUE NONCLUSTERED ([blake3] ASC);
        """)
        cursor.execute(f"""
            ALTER TABLE [dbo].[{target_table}]
            ADD CONSTRAINT [UQ_{target_table}_file_path] UNIQUE NONCLUSTERED ([file_path] ASC);
        """)
        for htype in ['dhash','phash','whash','chash','ahash']:
            cursor.execute(f"""
                CREATE NONCLUSTERED INDEX [IX_{target_table}_{htype}]
                ON [dbo].[{target_table}] ([{htype}] ASC);
            """)
        # Add index on is_representative for efficient filtering
        cursor.execute(f"""
            CREATE NONCLUSTERED INDEX [IX_{target_table}_is_representative]
            ON [dbo].[{target_table}] ([is_representative] ASC);
        """)
        # Add index on file_directory for efficient grouping
        cursor.execute(f"""
            CREATE NONCLUSTERED INDEX [IX_{target_table}_file_directory]
            ON [dbo].[{target_table}] ([file_directory] ASC);
        """)
        connection.commit()
        self.logger.info(f"[bright_black][Deduper]📸[/bright_black][bold green] Table '{target_table}' created successfully.[/bold green]")

    def _extract_row_data(self, row: pyodbc.Row):
        """
        Return a tuple (col_names, col_values) for the base schema from your examples.
        """
        col_names = [
            "file_path","file_name","file_directory","file_type","file_extension",
            "file_size","md5","sha256","sha512","blake3",
            "dhash","phash","whash","chash","ahash",
            "video_fingerprint","video_width","video_height","video_resolution",
            "video_fps","video_length","has_human","has_human_score","has_human_count","date"
        ]
        col_values = [
            row.file_path,
            row.file_name,
            row.file_directory,
            row.file_type,
            row.file_extension,
            row.file_size,
            row.md5,
            row.sha256,
            row.sha512,
            row.blake3,
            row.dhash,
            row.phash,
            row.whash,
            row.chash,
            row.ahash,
            row.video_fingerprint,
            row.video_width,
            row.video_height,
            row.video_resolution,
            row.video_fps,
            row.video_length,
            row.has_human,
            row.has_human_score,
            row.has_human_count,
            row.date
        ]
        return (col_names, col_values)