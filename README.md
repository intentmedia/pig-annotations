## pig-annotations

_pig-annotations_ is a class library that makes it easy to load your
custom serialized java objects into [pig][1] as proper pig Tuples with a
well-defined schema.

### Should I use _pig-annotations_?

_pig-annotations_ has a rather narrow scope.  You should probably only
use _pig-annotations_ if the following is true:

* You use java.  _pig-annotations_ is a java library.
* You use [pig][1].
* You already have a custom means of serializing your java objects in a **line-based** text format (like json).

### How do I use _pig-annotations_?

Using _pig-annotations_ is straightforward.  

* You will need to provide an implementation of [RecordInflater][2] to convert 
  from a line of text into your java object.  This implementation must have a 
  no-arg public constructor.
* You will need to annotate your object to specify how to convert the
  fields into values within a tuple.

## Example

It is probably easiest to demonstrate via an example.  

Let's say you have a *Person* class that has two fields, age and gender.

```java
package com.intentmedia.examples;

public class Person {
    private String gender;
    private Integer age;

    // getters, setters, etc.
}
```

You need to tell _pig-annotations_ how to transform each field.

```java
package com.intentmedia.examples;

import com.intentmedia.pig.PigField;

import static org.apache.pig.data.DataType.CHARARRAY;
import static org.apache.pig.data.DataType.INTEGER;

public class Person {

    @PigField(name = "gender", type = CHARARRAY)
    private String gender;


    @PigField(name = "gender", type = INTEGER)
    private Integer age;

    // getters, setters, etc.
}
```

For each field, you supply a name, and what Pig data type to map it to.

Finally, you need to tell _pig-annotations_ how to load your object
before it can turn it into a pig tuple.  If your objects were stored as
a csv like this:

```
male,25
female,26
```

Then you need to implement `RecordInflater<Person>`.

```java
package com.intentmedia.examples;

import com.intentmedia.examples.Person;
import com.intentmedia.convert.RecordInflater;
import org.apache.hadoop.io.Text;
import org.jetbrains.annotations.NotNull;

public class PersonFromCsvInflater implements RecordInflater<Person> {
    @NotNull
    @Override
    public Person convert(@NotNull Text value) throws IllegalArgumentException {

        String[] genderAndAge = value.toString().split(",");

        Person person = new Person();
        person.setGender(genderAndAge[0]);
        person.setAge(Integer.parseInt(genderAndAge[1]));

        return person;
    }
}
```

Finally, just add one more annotation to your _Person_ class.

```java
package com.intentmedia.examples;

import com.intentmedia.pig.PigField;

import static org.apache.pig.data.DataType.CHARARRAY;
import static org.apache.pig.data.DataType.INTEGER;

@PigLoadable(recordInflater = PersonFromCsvInflater.class)
public class Person {

    @PigField(name = "gender", type = CHARARRAY)
    private String gender;


    @PigField(name = "gender", type = INTEGER)
    private Integer age;

    // getters, setters, etc.
}
```

Now, to load your objects via pig, you would use a load function like:


```
REQUIRE 'location/to/pig-annotations.jar'
REQUIRE 'your/jar/with/other/classes.jar'

people = LOAD 'your/input/files/*.csv' 
  USING com.intentmedia.pig.AnnotatedObjectLoader('com.intentmedia.examples.Person');

```

And the `people` alias will have the pig schema `tuple(gender:chararray,age:int)`.

## But wait, there's more

_pig-annotations_ also supports the following features:

* Custom converters for fields that can't be autoboxed into pig types.
* Mapping Booleans to Integers (because Pig doesn't have booleans yet)
* Unwrapping fields annotated with @Embedded

[1]: http://pig.apache.org "Apache Pig"
[2]: https://github.com/intentmedia/pig-annotations/blob/master/src/main/java/com/intentmedia/convert/RecordInflater.java
