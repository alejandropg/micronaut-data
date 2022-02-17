package io.micronaut.data.document.mongodb

import com.mongodb.client.MongoClient
import groovy.transform.Memoized
import io.micronaut.data.document.mongodb.repositories.MongoAuthorRepository
import io.micronaut.data.document.mongodb.repositories.MongoBasicTypesRepository
import io.micronaut.data.document.mongodb.repositories.MongoBookRepository
import io.micronaut.data.document.mongodb.repositories.MongoDomainEventsRepository
import io.micronaut.data.document.mongodb.repositories.MongoPersonRepository
import io.micronaut.data.document.mongodb.repositories.MongoSaleRepository
import io.micronaut.data.document.mongodb.repositories.MongoStudentRepository
import io.micronaut.data.document.tck.AbstractDocumentRepositorySpec
import io.micronaut.data.document.tck.entities.Quantity
import io.micronaut.data.document.tck.entities.Sale
import io.micronaut.data.document.tck.repositories.AuthorRepository
import io.micronaut.data.document.tck.repositories.BasicTypesRepository
import io.micronaut.data.document.tck.repositories.BookRepository
import io.micronaut.data.document.tck.repositories.DomainEventsRepository
import io.micronaut.data.document.tck.repositories.SaleRepository
import io.micronaut.data.document.tck.repositories.StudentRepository
import org.bson.BsonDocument

class MongoDocumentRepositorySpec extends AbstractDocumentRepositorySpec implements MongoTestPropertyProvider {

    MongoClient mongoClient = context.getBean(MongoClient)

    void "test id mapping"() {
        given:
            savePersons(["Dennis", "Jeff", "James", "Dennis"])
        when:
            def people = personRepository.queryAll()
        then:
            people.every {it.size() == 4 && !it.id }
    }

    void "test attribute converter"() {
        when:
            def quantity = new Quantity(123)
            def sale = new Sale()
            sale.quantity = quantity
            saleRepository.save(sale)

            sale = saleRepository.findById(sale.getId()).get()
        then:
            sale.quantity
            sale.quantity.amount == 123

        when:
            def bsonSale = mongoClient.getDatabase("test").getCollection("sale", BsonDocument).find().first()
        then:
            bsonSale.size() == 2
            bsonSale.get("quantity").isInt32()

        when:
            saleRepository.updateQuantity(sale.id, new Quantity(345))
            sale = saleRepository.findById(sale.id).get()
        then:
            sale.quantity
            sale.quantity.amount == 345

        cleanup:
            saleRepository.deleteById(sale.id)
    }

    void "test custom update"() {
        given:
            savePersons(["Dennis", "Jeff", "James", "Dennis"])

        when:
            personRepository.updateNamesCustom("Denis", "Dennis")
            def people = personRepository.findAll().toList()

        then:
            people.count { it.name == "Dennis"} == 0
            people.count { it.name == "Denis"} == 2
    }

    void "test custom update single"() {
        given:
            savePersons(["Dennis", "Jeff", "James", "Dennis"])

        when:
            def people = personRepository.findAll().toList()
            def jeff = people.find {it.name == "Jeff"}
            def updated = personRepository.updateCustomSingle(jeff)
            people = personRepository.findAll().toList()

        then:
            updated == 1
            people.count {it.name == "tom"} == 1
    }

    void "test custom update only names"() {
        when:
            savePersons(["Dennis", "Jeff", "James", "Dennis"])
            def people = personRepository.findAll().toList()
            people.forEach {it.age = 100 }
            personRepository.updateAll(people)
            people = personRepository.findAll().toList()

        then:
            people.size() == 4
            people.every{it.age > 0 }

        when:
            people.forEach() {
                it.name = it.name + " updated"
                it.age = -1
            }
            int updated = personRepository.updateCustomOnlyNames(people)
            people = personRepository.findAll().toList()

        then:
            updated == 4
            people.size() == 4
            people.every {it.name.endsWith(" updated") }
            people.every {it.age > 0 }
    }

    void "test custom delete"() {
        given:
            savePersons(["Dennis", "Jeff", "James", "Dennis"])

        when:
            def people = personRepository.findAll().toList()
            people.findAll {it.name == "Dennis"}.forEach{ it.name = "DoNotDelete"}
            def deleted = personRepository.deleteCustom(people)
            people = personRepository.findAll().toList()

        then:
            deleted == 2
            people.size() == 2
            people.count {it.name == "Dennis"}
    }

    void "test custom delete single"() {
        given:
            savePersons(["Dennis", "Jeff", "James", "Dennis"])

        when:
            def people = personRepository.findAll().toList()
            def jeff = people.find {it.name == "Jeff"}
            def deleted = personRepository.deleteCustomSingle(jeff)
            people = personRepository.findAll().toList()

        then:
            deleted == 1
            people.size() == 3

        when:
            def james = people.find {it.name == "James"}
            james.name = "DoNotDelete"
            deleted = personRepository.deleteCustomSingle(james)
            people = personRepository.findAll().toList()

        then:
            deleted == 0
            people.size() == 3
    }

    void "test custom delete single no entity"() {
        given:
            savePersons(["Dennis", "Jeff", "James", "Dennis"])

        when:
            def people = personRepository.findAll().toList()
            def jeff = people.find {it.name == "Jeff"}
            def deleted = personRepository.deleteCustomSingleNoEntity(jeff.getName())
            people = personRepository.findAll().toList()

        then:
            deleted == 1
            people.size() == 3
    }

    @Memoized
    @Override
    BasicTypesRepository getBasicTypeRepository() {
        return context.getBean(MongoBasicTypesRepository)
    }

    @Memoized
    @Override
    MongoPersonRepository getPersonRepository() {
        return context.getBean(MongoPersonRepository)
    }

    @Memoized
    @Override
    BookRepository getBookRepository() {
        return context.getBean(MongoBookRepository)
    }

    @Memoized
    @Override
    AuthorRepository getAuthorRepository() {
        return context.getBean(MongoAuthorRepository)
    }

    @Memoized
    @Override
    StudentRepository getStudentRepository() {
        return context.getBean(MongoStudentRepository)
    }

    @Memoized
    @Override
    SaleRepository getSaleRepository() {
        return context.getBean(MongoSaleRepository)
    }

    @Memoized
    @Override
    DomainEventsRepository getEventsRepository() {
        return context.getBean(MongoDomainEventsRepository)
    }
}
