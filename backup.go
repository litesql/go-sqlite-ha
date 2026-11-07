package sqliteha

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/litesql/go-ha"
)

func Backup(ctx context.Context, db *sql.DB, w io.Writer) error {
	return ha.BackupWithAdapter(
		ctx,
		db,
		w,
		func(sqliteSrcConn SQLiteConn) ([]byte, error) {
			return sqliteSrcConn.Serialize()
		},
		func(_ context.Context, sqliteSrcConn SQLiteConn, destName string) error {
			bkp, err := sqliteSrcConn.NewBackup(destName)
			if err != nil {
				return err
			}

			for more := true; more; {
				more, err = bkp.Step(-1)
				if err != nil {
					return fmt.Errorf("backup step error: %w", err)
				}
			}

			if err := bkp.Finish(); err != nil {
				return fmt.Errorf("backup finish error: %w", err)
			}
			return nil
		},
	)
}
