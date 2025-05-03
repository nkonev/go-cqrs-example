package main

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	_ "github.com/jackc/pgx/v4/stdlib"
	"log/slog"
	"net/http"
)

type DB struct {
	*sql.DB
	lgr *slog.Logger
}

type Tx struct {
	*sql.Tx
	lgr *slog.Logger
}

type CommonOperations interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func (dbR *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return dbR.DB.QueryContext(ctx, query, args...)
}

func (txR *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return txR.Tx.QueryContext(ctx, query, args...)
}

func (dbR *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return dbR.DB.QueryRowContext(ctx, query, args...)
}

func (txR *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return txR.Tx.QueryRowContext(ctx, query, args...)
}

func (dbR *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return dbR.DB.ExecContext(ctx, query, args...)
}

func (txR *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return txR.Tx.ExecContext(ctx, query, args...)
}

// Begin starts and returns a new transaction.
func (db *DB) Begin(ctx context.Context, lgr *slog.Logger) (*Tx, error) {
	if tx, err := db.DB.BeginTx(ctx, nil); err != nil {
		return nil, fmt.Errorf("error during interacting with db: %w", err)
	} else {
		return &Tx{tx, lgr}, nil
	}
}

func (tx *Tx) SafeRollback() {
	if err0 := tx.Rollback(); err0 != nil {
		tx.lgr.Error("Error during rollback tx ", "err", err0)
	}
}

func TransactWithResult[T any](ctx context.Context, db *DB, txFunc func(*Tx) (T, error)) (ret T, err error) {
	tx, err := db.Begin(ctx, db.lgr)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()
	ret, err = txFunc(tx)
	return ret, err
}

func Transact(ctx context.Context, db *DB, txFunc func(*Tx) error) (err error) {
	tx, err := db.Begin(ctx, db.lgr)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()
	err = txFunc(tx)
	return err
}

//go:embed migrations
var embeddedMigrationFiles embed.FS

func (db *DB) Migrate(mc MigrationConfig) error {
	db.lgr.Info("Starting migration")
	staticDir := http.FS(embeddedMigrationFiles)
	src, err := httpfs.New(staticDir, "migrations")
	if err != nil {
		return err
	}

	pgInstance, err := postgres.WithInstance(db.DB, &postgres.Config{
		MigrationsTable:  mc.MigrationTable,
		StatementTimeout: mc.StatementDuration,
	})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithInstance("httpfs", src, "", pgInstance)
	if err != nil {
		return err
	}
	//defer m.Close()
	if err := m.Up(); err != nil && err.Error() != "no change" {
		return err
	}
	db.lgr.Info("Migration successfully completed")
	return nil
}
