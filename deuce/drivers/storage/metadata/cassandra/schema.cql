
CREATE COLUMNFAMILY BLOCKS
(
  project_id TEXT,
  block_id TEXT,
  block_size int,
  PRIMARY KEY(project_id, block_id) 
);

CREATE TABLE files
(
  project_id TEXT,
  file_id uuid,
  size bigint,
  finalized boolean,
  PRIMARY KEY(project_id, file_id)
);

CREATE TABLE fileblocks
(
  project_id TEXT,
  file_id text,
  block_id text,
  block_size int,
  offset bigint,
  PRIMARY KEY(project_id, file_id, offset)
);
