Building _pig-annotations_ is straightforward, but does require
[Maven][1] to be installed.

[1]: http://maven.apache.org "Maven"

```
$ git clone git@github.com/intentmedia/pig-annotations.git
$ cd pig-annotations
$ mvn package
```

That will compile, execute the unit tests, and leave a .jar file in
`pig-annotations/target`
