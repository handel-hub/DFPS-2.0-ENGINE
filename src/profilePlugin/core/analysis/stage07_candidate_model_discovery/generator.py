from typing import List
from profilePlugin.core.analysis.types.complexity_matrix import ComplexityClass, ClassifiedRelationship
from profilePlugin.core.analysis.types.candidate_models import ModelArchitecture, HypothesisModel

class HypothesisGenerator:
    """
    Generates tailored mathematical model templates according to strict asymptotic boundaries.
    """

    # Maps a complexity class strictly to its valid hypothesis architectures
    ARCHITECTURE_MAPPING = {
        ComplexityClass.CONSTANT: [
            ModelArchitecture.CONSTANT_MEAN,
            ModelArchitecture.CONSTANT_MEDIAN
        ],
        ComplexityClass.UNKNOWN: [
            ModelArchitecture.CONSTANT_MEAN,
            ModelArchitecture.CONSTANT_MEDIAN
        ],
        ComplexityClass.LINEAR: [
            ModelArchitecture.LINEAR_OLS,
            ModelArchitecture.LINEAR_ROBUST
        ],
        ComplexityClass.QUADRATIC: [
            ModelArchitecture.QUADRATIC_OLS
        ],
        ComplexityClass.LOG_LINEAR: [
            ModelArchitecture.LOG_LINEAR_OLS
        ]
    }

    @classmethod
    def generate_hypotheses(cls, relationship: ClassifiedRelationship) -> List[HypothesisModel]:
        """
        Translates a single classified relationship into multiple testable hypothesis bounds.
        """
        architectures = cls.ARCHITECTURE_MAPPING.get(relationship.complexity, cls.ARCHITECTURE_MAPPING[ComplexityClass.UNKNOWN])
        
        return [
            HypothesisModel(
                source=relationship.source,
                target=relationship.target,
                architecture=arch
            )
            for arch in architectures
        ]
