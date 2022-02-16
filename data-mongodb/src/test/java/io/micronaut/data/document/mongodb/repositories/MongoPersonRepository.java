package io.micronaut.data.document.mongodb.repositories;

import io.micronaut.data.document.tck.entities.Person;
import io.micronaut.data.document.tck.repositories.PersonRepository;
import io.micronaut.data.mongodb.annotation.MongoRepository;
import io.micronaut.data.mongodb.annotation.MongoUpdateQuery;
import org.bson.BsonDocument;

import java.util.List;

@MongoRepository
public interface MongoPersonRepository extends PersonRepository {

    List<BsonDocument> queryAll();

    @MongoUpdateQuery(update = "{$set:{name: :newName}}", filter = "{name:{$eq: :oldName}}")
    long updateNamesCustom(String newName, String oldName);

    @MongoUpdateQuery(update = "{$set:{name: :name}}", filter = "{_id:{$eq: :id}}")
    long updateCustomOnlyNames(List<Person> people);

//    @Query("DELETE FROM person WHERE name = :name")
//    int deleteCustom(List<Person> people);
//
//    @Query("DELETE FROM person WHERE name = :name")
//    int deleteCustomSingle(Person person);
//
//    @Query("DELETE FROM person WHERE name = :xyz")
//    int deleteCustomSingleNoEntity(String xyz);

}
