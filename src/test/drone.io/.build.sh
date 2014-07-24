## install arangodb

BUILDDIR=`pwd`

HOMEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $HOMEDIR

VERSION=2.2.0
NAME=ArangoDB-$VERSION

if [ ! -d "$DIR/$NAME" ]; then
  # download ArangoDB
  echo "wget http://www.arangodb.org/repositories/travisCI/$NAME.tar.gz"
  wget http://www.arangodb.org/repositories/travisCI/$NAME.tar.gz
  echo "tar zxf $NAME.tar.gz"
  tar zvxf $NAME.tar.gz
fi

ARCH=$(arch)
PID=$(echo $PPID)
TMP_DIR="/tmp/arangodb.$PID"
PID_FILE="/tmp/arangodb.$PID.pid"
ARANGODB_DIR="$HOMEDIR/$NAME"
ARANGOD="${ARANGODB_DIR}/bin/arangod_x86_64"

# create database directory
mkdir ${TMP_DIR}

echo "Starting ArangoDB '${ARANGOD}'"

${ARANGOD} \
    --database.directory ${TMP_DIR} \
    --configuration none \
    --server.endpoint tcp://127.0.0.1:8529 \
    --javascript.app-path ${ARANGODB_DIR}/js/apps \
    --javascript.startup-directory ${ARANGODB_DIR}/js \
    --database.maximal-journal-size 1048576 \
    --server.disable-authentication true &

sleep 2

echo "Check for arangod process"
process=$(ps auxww | grep "bin/arangod" | grep -v grep)

if [ "x$process" == "x" ]; then
  echo "no 'arangod' process found"
  echo "ARCH = $ARCH"
  exit 1
fi

echo "Waiting until ArangoDB is ready on port 8529"
while [[ -z `curl -s 'http://127.0.0.1:8529/_api/version' ` ]] ; do
  echo -n "."
  sleep 2s
done

echo "ArangoDB is up"

## maven build
echo "Starting MAVEN Build process"
cd $BUILDDIR
mvn install -q -DskipTests=true

## maven test
echo "Starting Tests"
mvn test -Dtest=santo.vertx.arangodb.integration.IntegrationTestSuite

## deploy to bintray
VERSION=`cat VERSION`
echo VERSION=$VERSION

if [[ "$GIT_BRANCH" = "master" ]] ; then
  echo "Master commit, not deploying"
elif [[ $VERSION == *SNAPSHOT* ]] ; then
  echo "Snapshot version, not deploying"
else
  echo "non-snapshot version, deploying to BinTray"

  cd target
  MODULE_NAME=`find vertx-arangodb-*-mod.zip`
  cd ..
  MODULE_FILE="target/$MODULE_NAME"

  curl -X PUT -u santo:<API-KEY> --data-binary @$MODULE_FILE "https://api.bintray.com/content/santo/vertx-mods/vertx-arangodb/$MODULE_NAME;bt_package=vertx-arangodb;bt_version=$VERSION"
fi
