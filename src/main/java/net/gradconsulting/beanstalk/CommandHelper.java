package net.gradconsulting.beanstalk;

class CommandHelper {

    public static CommandHandler reservedCommandHandler = (String ctrl) -> {
        if (ctrl.startsWith("RESERVED")) {
            try {
                return Integer.parseInt(ctrl.split(" ")[2].trim());
            } catch (NumberFormatException ex) {
                throw new BeansTalkException(ex.getMessage());
            }
        }

        if (ctrl.startsWith("DEADLINE_SOON"))
            throw new BeansTalkException("Dead line soon");

        if (ctrl.startsWith("TIMED_OUT"))
            throw new BeansTalkException("Time out");

        throw new BeansTalkException("Invalid response in reserve: " + ctrl);
    };

    public static CommandHandler peekCommandHandler = (String ctrl) -> {
        if (ctrl.startsWith("FOUND")) {
            try {
                return Integer.parseInt(ctrl.split(" ")[2].trim());
            } catch (NumberFormatException ex) {
                throw new BeansTalkException(ex.getMessage());
            }
        }
        if (ctrl.startsWith("NOT_FOUND"))
            return 0;

        throw new BeansTalkException("Invalid response in peek: " + ctrl);
    };

    public static CommandHandler statsJobCommandHandler = (String ctrl) -> {
        if (ctrl.startsWith("OK")) {
            try {
                return Integer.parseInt(ctrl.split(" ")[1].trim());
            } catch (NumberFormatException ex) {
                throw new BeansTalkException(ex.getMessage());
            }
        }

        if (ctrl.startsWith("NOT_FOUND"))
            throw new BeansTalkException("Job id not found");

        throw new BeansTalkException("Invalid response in stats-job: " + ctrl);
    };

    public static CommandHandler statsTubeCommandHandler = (String ctrl) -> {
        if (ctrl.startsWith("OK")) {
            try {
                return Integer.parseInt(ctrl.split(" ")[1].trim());
            } catch (NumberFormatException ex) {
                throw new BeansTalkException(ex.getMessage());
            }
        }
        if (ctrl.startsWith("NOT_FOUND"))
            throw new BeansTalkException("Tube not found");

        throw new BeansTalkException("Invalid response in stats-tube: " + ctrl);
    };

    public static CommandHandler statsCommandHandler = (String ctrl) -> {
        if (ctrl.startsWith("OK")) {
            try {
                return Integer.parseInt(ctrl.split(" ")[1].trim());
            } catch (NumberFormatException ex) {
                throw new BeansTalkException(ex.getMessage());
            }
        }
        throw new BeansTalkException("Invalid response in stats: " + ctrl);
    };

    public static CommandHandler listTubesCommandHandler = (String ctrl) -> {
        if (ctrl.startsWith("OK")) {
            try {
                return Integer.parseInt(ctrl.split(" ")[1].trim());
            } catch (NumberFormatException ex) {
                throw new BeansTalkException(ex.getMessage());
            }
        }
        throw new BeansTalkException("Invalid response in list-tubes: " + ctrl);
    };

    public static CommandHandler listTubesWatchedCommandHandler = (String ctrl) -> {
        if (ctrl.startsWith("OK")) {
            try {
                return Integer.parseInt(ctrl.split(" ")[1].trim());
            } catch (NumberFormatException ex) {
                throw new BeansTalkException(ex.getMessage());
            }
        }
        throw new BeansTalkException("Invalid response in list-tubes-watched: " + ctrl);
    };
}
