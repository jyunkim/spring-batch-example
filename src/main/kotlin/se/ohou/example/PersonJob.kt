package se.ohou.example

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.item.file.mapping.DefaultLineMapper
import org.springframework.batch.item.file.mapping.FieldSetMapper
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer
import org.springframework.batch.item.file.transform.FieldSet
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller
import org.springframework.batch.item.json.JsonFileItemWriter
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.FileSystemResource

@Configuration
class PersonJob(
    val jobBuilderFactory: JobBuilderFactory,
    val stepBuilderFactory: StepBuilderFactory
) {
    @Bean
    fun personProcessingJob(): Job = jobBuilderFactory.get("personProcessingJob")
        .flow(personProcessingStep())
        .end()
        .build()

    @Bean
    fun personProcessingStep(): Step {
        // reader, processor, writer를 가진 tasklet step을 생성
        return stepBuilderFactory.get("personProcessingStep")
            .chunk<Person, Person>(1)
            .reader(reader())
            .processor(processor())
            .writer(writer())
            .build()
    }

    @Bean
    fun reader(): ItemReader<Person> {
        // sample-data.csv 파일을 읽어서 Person 도메인 객체로 컨버팅
        val itemReader = FlatFileItemReader<Person>()
        itemReader.setResource(FileSystemResource("src/main/resources/sample-data.csv"))

        val lineMapper = DefaultLineMapper<Person>()
        lineMapper.setLineTokenizer(DelimitedLineTokenizer()) // 기본 구분자 = ,
        lineMapper.setFieldSetMapper(PersonFieldSetMapper())

        itemReader.setLineMapper(lineMapper)
        return itemReader
    }

    @Bean
    fun processor(): ItemProcessor<Person, Person> {
        // Person 객체안에 name을 모두 소문자로 변환
        return PersonProcessor()
    }

    @Bean
    fun writer(): ItemWriter<Person> {
        // Person 객체를 json 파일로 write (파일 위치는 상관없음)
        return JsonFileItemWriterBuilder<Person>()
            .jsonObjectMarshaller(JacksonJsonObjectMarshaller())
            .resource(FileSystemResource("src/main/resources/sample-data.json"))
            .name("personJsonFileItemWriter")
            .build()
    }

    class PersonFieldSetMapper : FieldSetMapper<Person> {
        override fun mapFieldSet(fieldSet: FieldSet) =
            Person(fieldSet.readString(0), fieldSet.readInt(1))
    }

    class PersonProcessor : ItemProcessor<Person, Person> {
        override fun process(item: Person): Person {
            val newName = item.name.lowercase()
            return Person(newName, item.age)
        }
    }
}