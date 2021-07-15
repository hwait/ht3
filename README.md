# otus-ht3
## Задача: Использование Spark Streaming для потокового обучения моделей 

### Реализованы:
- загрузка данных по дням и стриминг их в Кафку батчами с заданной длительностью и периодичностью;
- получение данных из Кафки;
- преобразование данных с помщью пайплайна;
- расчёт линейной регрессии окнами с заданной длительностью;
- вывод для каждого батча RMSE и R2 метрик.

**NB!** Никакой предсказательной функции проект не имеет, лишь подсчет и вывод коэффициентов линейной регрессии и метрик в стриминге. 

### Краткое описание файлов:
- `resources/application.json` - файл ноутбука *explore.ipynb*;
- `DataColumns.scala` - Данные об именах столбцов и преобразования их.
- `DataProducer.scala` - Стриминг данных
- `DataProvider.scala` - Провайдер данных для стриминга
- `DataTransformer.scala` - Пайплайн данных
- `OneHotEncoderToKnownClassesNumber.scala` - Кастомный OneHotEncoder
- `ProjectConfiguration.scala` - работа с конфигурационным файлом
- `SparkMLModelTraining.scala` - проверка работы модели на локальных данных (без стриминга)
- `SparkMLRegressionFromKafka.scala` - Получает данные из Кафки, преобразует их с помщью пайплайна, считает линейную регрессию окнами с заданной длительностью. Выводит для каждого батча RMSE и R2 метрики.

- `data/` - данные из папки *train* (не включены в проект) датасета https://www.kaggle.com/sharthz23/sna-hackathon-2019-collaboration?select=train. 

