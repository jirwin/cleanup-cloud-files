var pkgcloud = require('pkgcloud');
var async = require('async');
var prettyBytes = require('pretty-bytes');
var chalk = require('chalk');

var argv = require('minimist')(process.argv.slice(2));

if (argv.help || !argv.user || !argv.apiKey || !argv.region || !argv.ageWindow) {
  console.log('Usage: $0 --user <username> --apiKey <apiKey> --region <rackspace region> --ageWindow <the max age of a container to keep (ms)>');
  console.log('\tAll runs default to a dry run. Pass --kamikaze to enable destructive behavior.');
  process.exit(1);
}

if (argv.kamikaze) {
  console.log(chalk.red.bold('ENABLING DESTRUCTIVE BEHAVIOR'));
}

var rackspace = pkgcloud.storage.createClient({
  provider: 'rackspace',
  username: argv.user,
  apiKey: argv.apiKey,
  region: argv.region
});

var totalDeleted = 0,
    doneDeleting = false;

var deleteQueue = async.queue(function(container, callback) {
  if (!container) {
    doneDeleting = true;
    process.nextTick(callback);
    return;
  }
  if (!argv.kamikaze) {
    console.log(chalk.yellow('Would delete container:', container.name));
    totalDeleted += container.bytes;
    process.nextTick(callback);
    return;
  }
  console.log(chalk.green('Deleting container:', container.name));
  rackspace.destroyContainer(container.name, function(err, result) {
    if (err) {
      console.error(chalk.red('Error deleting container:', container.name, err));
    }

    if (result) {
      totalDeleted += container.bytes;
      console.log(chalk.green('Successfully deleted container:', container.name));
    }
    callback();
  });
}, 10);

deleteQueue.drain = function() {
  if (doneDeleting) {
    deleteQueue.kill();
    console.log(chalk.green('Total of ' + prettyBytes(totalDeleted) + ' removed.'));
  }
};

async.auto({
  containers: function getContainers(callback) {
    console.log(chalk.yellow('Grabbing containers from Cloud Files...'));
    rackspace.getContainers(function(err, containers) {
      var containersReturn = {};

      containers.forEach(function(c) {
        containersReturn[c.name] = c;
      });
      callback(err, containersReturn);
    });
  },

  lastModified: ['containers', function getLastModified(callback, results) {
    var containers = results.containers,
      now = Date.now();

    if (!containers.length) {
      callback();
      return;
    }

    console.log(chalk.yellow('Checking for the last time each container was touched...'));
    async.forEach(Object.keys(containers), function(c, callback) {
      rackspace.getFiles(c, {limit: Infinity}, function(err, files) {
        var newestFileAge;

        if (err) {
          callback(err);
          return;
        }

        newestFileAge = files.reduce(function(prev, f) {
          var age = now - Date.parse(f.lastModified);

          if (age < prev) {
            return age;
          }
          return prev;
        }, Number.MAX_VALUE);

        if (newestFileAge > argv.ageWindow) {
          deleteQueue.push({name: containers[c].name, bytes: containers[c].bytes});
        } else {
          console.log(chalk.yellow('Skipping ' + c + '.'));
        }
        callback();
      });
    }, function(err) {
      deleteQueue.push(false);
      callback(err);
    });
  }]
}, function(err, results) {
  if (err) {
    console.error(chalk.red.bold('An error occurred:', err));
    process.exit(1);
  }

  if (results.containers) {
    console.log(chalk.green('No containers found. Closing.'));
  }
});
