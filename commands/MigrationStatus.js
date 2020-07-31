"use strict";

/*
 * adonis-lucid
 *
 * (c) Harminder Virk <virk@adonisjs.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

const BaseMigration = require("./BaseMigration");

class MigrationStatus extends BaseMigration {
  /**
   * Command signature required by ace
   *
   * @method signature
   *
   * @return {String}
   */
  static get signature() {
    return `
    migration:status
    { -a, --keep-alive: Do not close the database connection }
    `;
  }

  /**
   * Command description
   *
   * @method description
   *
   * @return {String}
   */
  static get description() {
    return "Check migrations current status";
  }

  /**
   * Method called when command is executed. This method
   * will print a table with the migrations status.
   *
   * @method handle
   *
   * @param  {Object} args
   * @param  {Boolean} options.keepAlive
   *
   * @return {void|Array}
   */
  async handle(args, { keepAlive }) {
    if (keepAlive) {
      this.migration.keepAlive();
    }

    try {
      // EMM: added connectionName property (undefined by default) to filter out migrations by connection
      const connectionName = args ? args.connectionName : undefined;
      const migrations = await this.migration.status(
        this._getSchemaFiles(connectionName),connectionName
      );
      
      if (this.viaAce) {
        const head = ["File name", "Migrated", "Batch"];
        const body = migrations.map((migration) => {
          return [
            migration.name,
            migration.migrated ? "Yes" : "No",
            migration.batch || "",
          ];
        });
        this.table(head, body);
      } else {
        return migrations.map((migration) => {
          return {
            name: migration.name,
            migrated: migration.migrated ? true : false,
            batch: migration.batch || null,
          };
        });
      }
    } catch (error) {
      if (!this.viaAce) {
        throw new Error(error);
      } else {
        console.log(error);
      }
    }
  }
}

module.exports = MigrationStatus;
