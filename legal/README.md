## Eclipse Ditto Testing :: Legal

This folder contains license-information of Ditto Testing itself and used third-parties.

### Update NOTICE-THIRD-PARTY.md

Generate/update the file [NOTICE-THIRD-PARTY.md](NOTICE-THIRD-PARTY.md) like this:

```bash
$ cd ../ # switch to / dir
$ mvn generate-resources -Pgenerate-third-party-licenses
``` 

This will update the [NOTICE-THIRD-PARTY.md](NOTICE-THIRD-PARTY.md) according to the actually used dependencies 
including the license information.
