import logging
import logging.config

import ServerSideExtension_pb2 as SSE
import grpc
import numpy
from SSEData_script import ArgType, \
                           FunctionType, \
                           ReturnType


class ScriptEval:
    """
    Class for SSE plugin ScriptEval functionality.
    """

    def EvaluateScript(self, header, request, context):
        """
        Evaluates script provided in the header, given the
        arguments provided in the sequence of RowData objects, the request.

        :param header:
        :param request: an iterable sequence of RowData.
        :param context: the context sent from client
        :return: an iterable sequence of RowData.
        """
        # Retrieve function type
        func_type = self.get_func_type(header)

        # Retrieve data types from header
        arg_types = self.get_arg_types(header)
        ret_type = self.get_return_type(header)

        logging.info('EvaluateScript: {} ({} {}) {}'
                     .format(header.script, arg_types, ret_type, func_type))

        # Check if parameters are provided
        if header.params:
            all_rows = []

            # Iterate over bundled rows
            for request_rows in request:
                # Iterate over rows
                for row in request_rows.rows:
                    # Retrieve parameters
                    params = self.get_arguments(context, arg_types, row.duals, header)
                    all_rows.append(params)

            # First element in the parameter list should contain the data of the first parameter.
            all_rows = [list(param) for param in zip(*all_rows)]

            if arg_types == ArgType.Mixed:
                param_datatypes = [param.dataType for param in header.params]
                for i, datatype in enumerate(param_datatypes):
                    if datatype == SSE.DUAL:
                        # For easier access to the numerical and string representation of duals, in the script, we
                        # split them to two list. For example, if the first parameter is dual, it will contain two lists
                        # the first one being the numerical representation and the second one the string.
                        all_rows[i] = [list(datatype) for datatype in zip(*all_rows[i])]

            logging.debug('Received data from Qlik (args): {}'.format(all_rows))
            yield self.evaluate(header.script, ret_type, params=all_rows)

        else:
            # No parameters provided
            yield self.evaluate(header.script, ret_type)

    @staticmethod
    def get_func_type(header):
        """
        Retrieves the function type.
        :param header:
        :return:
        """
        func_type = header.functionType
        if func_type == SSE.SCALAR:
            return FunctionType.Scalar
        elif func_type == SSE.AGGREGATION:
            return FunctionType.Aggregation
        elif func_type == SSE.TENSOR:
            return FunctionType.Tensor

    @staticmethod
    def get_arguments(context, arg_types, duals, header):
        """
        Gets the array of arguments based on
        the duals, and the type (string, numeric)
        specified in the header.
        :param context: the context sent from client
        :param header: the script header.
        :param duals: an iterable sequence of duals.
        :return: an array of (potentially mixed data type) arguments.
        """

        if arg_types == ArgType.String:
            # All parameters are of string type
            script_args = [d.strData for d in duals]
        elif arg_types == ArgType.Numeric:
            # All parameters are of numeric type
            script_args = [d.numData for d in duals]
        elif arg_types == ArgType.Mixed:
            # Parameters can be either string, numeric or dual
            script_args = []
            for dual, param in zip(duals, header.params):
                if param.dataType == SSE.STRING:
                    script_args.append(dual.strData)
                elif param.dataType == SSE.NUMERIC:
                    script_args.append(dual.numData)
                elif param.dataType == SSE.DUAL:
                    script_args.append((dual.numData, dual.strData))
        else:
            # Undefined argument types
            # Make sure the error handling, including logging, works as intended in the client
            msg = 'Undefined argument type: '.format(arg_types)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            # Raise error on the plugin-side
            raise grpc.RpcError(grpc.StatusCode.INVALID_ARGUMENT, msg)
        return script_args

    @staticmethod
    def get_arg_types(header):
        """
        Determines the argument types for all parameters.
        :param header:
        :return: ArgType
        """
        data_types = [param.dataType for param in header.params]

        if not data_types:
            return ArgType.Empty
        elif len(set(data_types)) > 1 or all(data_type == SSE.DUAL for data_type in data_types):
            return ArgType.Mixed
        elif all(data_type == SSE.STRING for data_type in data_types):
            return ArgType.String
        elif all(data_type == SSE.NUMERIC for data_type in data_types):
            return ArgType.Numeric
        else:
            return ArgType.Undefined

    @staticmethod
    def get_return_type(header):
        """
        :param header:
        :return: Return type
        """
        if header.returnType == SSE.STRING:
            return ReturnType.String
        elif header.returnType == SSE.NUMERIC:
            return ReturnType.Numeric
        elif header.returnType == SSE.DUAL:
            return ReturnType.Dual
        else:
            return ReturnType.Undefined

    @staticmethod
    def get_duals(result, ret_type):
        if isinstance(result, str) or not hasattr(result, '__iter__'):
            result = [result]
        # Transform the result to an iterable of Dual data
        if ret_type == ReturnType.String:
            duals = [SSE.Dual(strData=col) for col in result]
        elif ret_type == ReturnType.Numeric:
            duals = [SSE.Dual(numData=col) for col in result]
        return iter(duals)

    def evaluate(self, script, ret_type, params=[]):
        """
        Evaluates a script with given parameters and construct the result to a Row of duals.
        :param script:  script to evaluate
        :param ret_type: return data type
        :param params: params to evaluate. Default: []
        :return: a RowData of string dual
        """
        # Evaluate script
        result = eval(script, {'args': params, 'numpy': numpy})
        logging.debug('Result: {}'.format(result))

        bundledRows = SSE.BundledRows()
        if isinstance(result, str) or not hasattr(result, '__iter__'):
            # A single value is returned
            bundledRows.rows.add(duals=self.get_duals(result, ret_type))
        else:
            for row in result:
                # note that each element of the result should represent a row
                bundledRows.rows.add(duals=self.get_duals(row, ret_type))

        return bundledRows

