# migrations/dvsg_schema.py

"""
Schema / migration for device_video_shop_group and related bits.
Enhanced with: rotation, fit_mode, content_type, display_duration, display_order,
last_online_at, counter reset dates, and device_logs table.

NEW:
- device_temperature table for time-series temperature reporting (line graph)
"""

def ensure_dvsg_schema(conn):
    """
    Ensure the schema for device_video_shop_group and related triggers/indexes exists.
    Expects an open psycopg2 connection.
    """
    ddl_fn = """
    CREATE OR REPLACE FUNCTION public.set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """

    ddl_device_cols = """
    DO $$
    DECLARE t text;
    BEGIN
      -- connection
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='connection';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN connection BOOLEAN NOT NULL DEFAULT FALSE;
      END IF;

      -- screen
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='screen';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN screen TEXT;
      END IF;

      -- download_status
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='download_status';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN download_status BOOLEAN NOT NULL DEFAULT FALSE;
      END IF;

      -- is_online
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='is_online';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN is_online BOOLEAN NOT NULL DEFAULT FALSE;
      END IF;

      -- temperature (DOUBLE PRECISION) - latest snapshot value on device table
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='temperature';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN temperature DOUBLE PRECISION DEFAULT 0;
      END IF;

      -- daily_count (INT)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='daily_count';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN daily_count INT NOT NULL DEFAULT 0;
      END IF;

      -- monthly_count (INT)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='monthly_count';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN monthly_count INT NOT NULL DEFAULT 0;
      END IF;

      ----------------------------------------------------------------
      -- NEW FIELDS FOR ENHANCED REQUIREMENTS
      ----------------------------------------------------------------

      -- last_online_at (for 1-minute online threshold)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_online_at';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_online_at TIMESTAMPTZ;
      END IF;

      -- last_daily_reset (for counter reset logic)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_daily_reset';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_daily_reset DATE DEFAULT CURRENT_DATE;
      END IF;

      -- last_monthly_reset (for counter reset logic)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_monthly_reset';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_monthly_reset DATE DEFAULT DATE_TRUNC('month', CURRENT_DATE)::DATE;
      END IF;

    END $$;
    """

    ddl_video_cols = """
    DO $$
    DECLARE t text;
    BEGIN
      -- rotation (0, 90, 180, 270)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='rotation';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN rotation INT NOT NULL DEFAULT 0;
      END IF;

      -- content_type (video, image, html, pdf)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='content_type';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN content_type TEXT NOT NULL DEFAULT 'video';
      END IF;

      -- display_duration (seconds for non-video content)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='display_duration';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN display_duration INT DEFAULT 10;
      END IF;

      -- fit_mode (contain, cover, fill, none)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='fit_mode';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN fit_mode TEXT NOT NULL DEFAULT 'cover';
      END IF;
    END $$;
    """

    ddl_link_table = """
    CREATE TABLE IF NOT EXISTS public.device_video_shop_group (
      id          BIGSERIAL PRIMARY KEY,
      did         BIGINT     NOT NULL,
      vid         BIGINT     NOT NULL,
      sid         BIGINT     NOT NULL,
      gid         BIGINT,  -- NULL -> No group
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      dl_status   BOOLEAN NOT NULL DEFAULT FALSE,
      dl_at       TIMESTAMPTZ,
      dl_error    TEXT,
      CONSTRAINT dvsg_device_fk FOREIGN KEY (did) REFERENCES public.device(id) ON DELETE RESTRICT,
      CONSTRAINT dvsg_video_fk  FOREIGN KEY (vid) REFERENCES public.video(id)  ON DELETE RESTRICT,
      CONSTRAINT dvsg_shop_fk   FOREIGN KEY (sid) REFERENCES public.shop(id)   ON DELETE RESTRICT,
      CONSTRAINT dvsg_group_fk  FOREIGN KEY (gid) REFERENCES public."group"(id) ON DELETE RESTRICT
    );
    """

    ddl_link_cols = """
    DO $$
    DECLARE t text;
    BEGIN
      -- display_order (for multiple videos per screen)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_video_shop_group' AND column_name='display_order';
      IF NOT FOUND THEN
        ALTER TABLE public.device_video_shop_group ADD COLUMN display_order INT NOT NULL DEFAULT 0;
      END IF;
    END $$;
    """

    ddl_logs_table = """
    CREATE TABLE IF NOT EXISTS public.device_logs (
      id         BIGSERIAL PRIMARY KEY,
      did        BIGINT NOT NULL,
      log_type   TEXT NOT NULL,  -- 'temperature', 'door_open'
      value      DOUBLE PRECISION,
      logged_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT device_logs_device_fk FOREIGN KEY (did) REFERENCES public.device(id) ON DELETE CASCADE
    );
    """

    ddl_logs_indexes = """
    CREATE INDEX IF NOT EXISTS idx_device_logs_did ON public.device_logs(did);
    CREATE INDEX IF NOT EXISTS idx_device_logs_type ON public.device_logs(log_type);
    CREATE INDEX IF NOT EXISTS idx_device_logs_logged_at ON public.device_logs(logged_at);
    CREATE INDEX IF NOT EXISTS idx_device_logs_did_type_time ON public.device_logs(did, log_type, logged_at);
    """

    # ------------------------------------------------------------------
    # NEW: device_temperature table for time-series reporting
    # ------------------------------------------------------------------
    ddl_temperature_table = """
    CREATE TABLE IF NOT EXISTS public.device_temperature (
      id          BIGSERIAL PRIMARY KEY,
      did         BIGINT NOT NULL,
      temperature DOUBLE PRECISION NOT NULL,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT device_temperature_device_fk FOREIGN KEY (did) REFERENCES public.device(id) ON DELETE CASCADE
    );
    """

    ddl_temperature_indexes = """
    CREATE INDEX IF NOT EXISTS idx_device_temperature_did ON public.device_temperature(did);
    CREATE INDEX IF NOT EXISTS idx_device_temperature_recorded_at ON public.device_temperature(recorded_at);
    CREATE INDEX IF NOT EXISTS idx_device_temperature_did_recorded_at ON public.device_temperature(did, recorded_at DESC);
    """

    drop_old_uniq = """
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname='device_video_shop_group_uniq'
          AND conrelid='public.device_video_shop_group'::regclass
      ) THEN
        ALTER TABLE public.device_video_shop_group
          DROP CONSTRAINT device_video_shop_group_uniq;
      END IF;
    END $$;
    """

    ddl_unique_indexes = """
    CREATE UNIQUE INDEX IF NOT EXISTS dvsg_unique_with_gid
      ON public.device_video_shop_group (did, vid, sid, gid)
      WHERE (gid IS NOT NULL);

    CREATE UNIQUE INDEX IF NOT EXISTS dvsg_unique_no_gid
      ON public.device_video_shop_group (did, vid, sid)
      WHERE (gid IS NULL);
    """

    ddl_trigger = """
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_dvsg_updated_at') THEN
        CREATE TRIGGER trg_dvsg_updated_at
        BEFORE UPDATE ON public.device_video_shop_group
        FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
      END IF;
    END $$;
    """

    ddl_indexes = """
    CREATE INDEX IF NOT EXISTS idx_dvsg_did        ON public.device_video_shop_group(did);
    CREATE INDEX IF NOT EXISTS idx_dvsg_vid        ON public.device_video_shop_group(vid);
    CREATE INDEX IF NOT EXISTS idx_dvsg_sid        ON public.device_video_shop_group(sid);
    CREATE INDEX IF NOT EXISTS idx_dvsg_gid        ON public.device_video_shop_group(gid);
    CREATE INDEX IF NOT EXISTS idx_dvsg_did_status ON public.device_video_shop_group(did, dl_status);
    CREATE INDEX IF NOT EXISTS idx_dvsg_display_order ON public.device_video_shop_group(display_order);
    """

    with conn.cursor() as cur:
        cur.execute(ddl_fn)
        cur.execute(ddl_device_cols)
        cur.execute(ddl_video_cols)
        cur.execute(ddl_link_table)
        cur.execute(ddl_link_cols)
        cur.execute(ddl_logs_table)
        cur.execute(ddl_logs_indexes)

        # NEW
        cur.execute(ddl_temperature_table)
        cur.execute(ddl_temperature_indexes)

        cur.execute(drop_old_uniq)
        cur.execute(ddl_unique_indexes)
        cur.execute(ddl_trigger)
        cur.execute(ddl_indexes)

    conn.commit()
