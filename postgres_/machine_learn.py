from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
def get_data():
    iris_data = load_iris()

    # print(iris_data)
    # for i in iris_data:
    #     print(i)
    print("特征值：\n",iris_data.data)
    print("目标值：\n",iris_data.target)
    print("描述值：\n",iris_data.DESCR)


if __name__ == '__main__':
    get_data()