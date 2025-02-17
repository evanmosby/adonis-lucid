'use strict'

/*
 * adonis-lucid
 *
 * (c) Harminder Virk <virk@adonisjs.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
*/

const BaseMigration = require('./BaseMigration')
const _ = require('lodash')
const prettyHrTime = require('pretty-hrtime')

class MirationRollback extends BaseMigration {
  /**
   * Command signature required by ace
   *
   * @method signature
   *
   * @return {String}
   */
  static get signature () {
    return `
    migration:rollback
    { -b, --batch=@value: Rollback upto a specific batch number }
    { -f, --force: Forcefully run migrations in production }
    { -s, --silent: Silent the migrations output }
    { --log: Log SQL queries instead of executing them }
    { -a, --keep-alive: Do not close the database connection }
    `
  }

  /**
   * Command description
   *
   * @method description
   *
   * @return {String}
   */
  static get description () {
    return 'Rollback migration to latest batch or to a specific batch number'
  }

  /**
   * Method called when command is executed. This method will
   * require all files from the migrations directory
   * and rollback to a specific batch
   *
   * @method handle
   *
   * @param  {Object} args
   * @param  {Boolean} options.log
   * @param  {Boolean} options.force
   * @param  {Number} options.batch
   * @param  {Boolean} options.silent
   * @param  {Boolean} options.keepAlive
   *
   * @return {void|Array}
   */
  async handle (args, { log, force, batch, silent, keepAlive }) {
    try {
      batch = batch ? Number(batch) : null

      this._validateState(force)

      if (keepAlive) {
        this.migration.keepAlive()
      }

      const startTime = process.hrtime()
      const { migrated, status, queries } = await this.migration.down(this._getSchemaFiles(), batch, log)

      /**
       * Tell user that there is nothing to migrate
       */
      if (status === 'skipped') {
        this.execIfNot(silent, () => this.info('Already at the last batch'))
      }

      /**
       * Log files that been migrated successfully
       */
      if (status === 'completed' && !queries) {
        const endTime = process.hrtime(startTime)
        migrated.forEach((name) => this.execIfNot(silent, () => this.completed('rollback', `${name}.js`)))
        this.success(`Rollback completed in ${prettyHrTime(endTime)}`)
      }

      /**
       * If there are queries in the result, just log them
       */
      if (queries) {
        _.each(queries, ({ queries, name }) => {
          this.execIfNot(silent, () => console.log(this.chalk.magenta(`\n Queries for ${name}.js`)))
          _.each(queries, (query) => this.execIfNot(silent, () => console.log(`  ${query}`)))
          console.log('\n')
        })
      }

      if (!this.viaAce) {
        return { status, migrated, queries }
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

module.exports = MirationRollback
