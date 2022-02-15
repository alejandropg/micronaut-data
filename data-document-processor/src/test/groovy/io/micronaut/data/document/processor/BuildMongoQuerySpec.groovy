package io.micronaut.data.document.processor

import io.micronaut.data.mongodb.annotation.MongoSort

class BuildMongoQuerySpec extends AbstractDataSpec {

    void "test custom method"() {
        given:
            def repository = buildRepository('test.MyInterface2', """
import io.micronaut.data.mongodb.annotation.MongoRepository;
import io.micronaut.data.mongodb.annotation.MongoFindQuery;
import io.micronaut.data.document.tck.entities.Book;

@MongoRepository
@io.micronaut.context.annotation.Executable
interface MyInterface2 extends CrudRepository<Book, String> {

    @MongoFindQuery(\"$customQuery\")
    Book queryById(String id);

}
"""
            )

        when:
            String q = TestUtils.getQuery(repository.getRequiredMethod("queryById", String))
        then:
            q == storedQuery

        where:
            customQuery             || storedQuery
            '{_id:{$eq:\\"abc\\"}}' || '{_id:{$eq:"abc"}}'
            '{_id:{$eq:123}}'       || '{_id:{$eq:123}}'
            '{_id:{$eq: :id}}'       || '{_id:{$eq: {$mn_qp:0}}}'
    }

    void "test custom method2"() {
        given:
            def repository = buildRepository('test.MyInterface2', """
import io.micronaut.data.mongodb.annotation.*;
import io.micronaut.data.document.tck.entities.Book;

@MongoRepository
@io.micronaut.context.annotation.Executable
interface MyInterface2 extends CrudRepository<Book, String> {

    @MongoFindQuery(value = \"{_id:{\$eq:123}}\", sort = \"{ title : -1 }\")
    Book queryById(String id);

}
"""
            )

            def method = repository.getRequiredMethod("queryById", String)
        when:
            String q = TestUtils.getQuery(method)
        then:
            q == '{_id:{$eq:123}}'
        when:
            String sort = method.getAnnotation(MongoSort).stringValue().get()
        then:
            sort == "{ title : -1 }"
    }

}
