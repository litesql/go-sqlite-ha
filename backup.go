package sqliteha

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
)

func Backup(ctx context.Context, db *sql.DB, w io.Writer) error {
	srcConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer srcConn.Close()

	sqliteSrcConn, err := sqliteConn(srcConn)
	if err != nil {
		return err
	}

	var filename string
	err = srcConn.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name = ?", "main").Scan(&filename)
	if err != nil {
		return err
	}

	if filename == "" {
		// serialize memdb
		data, err := sqliteSrcConn.Serialize()
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	}

	dest, err := os.CreateTemp("", "ha-*.db")
	if err != nil {
		return err
	}
	defer os.Remove(dest.Name())

	bkp, err := sqliteSrcConn.NewBackup(dest.Name())
	if err != nil {
		return err
	}

	for more := true; more; {
		more, err = bkp.Step(-1)
		if err != nil {
			return fmt.Errorf("backup step error: %w", err)
		}
	}

	err = bkp.Finish()
	if err != nil {
		return fmt.Errorf("backup finish error: %w", err)
	}

	final, err := os.Open(dest.Name())
	if err != nil {
		return err
	}
	defer final.Close()

	_, err = io.Copy(w, final)
	return err
}
